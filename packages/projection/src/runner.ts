import {
  Array,
  Cause,
  Clock,
  Effect,
  Exit,
  Option,
  Predicate,
  PubSub,
  Queue,
  Record,
  Schema,
  type Schedule,
  Semaphore,
} from "effect";
import { Storage, StorageFault, type StreamId } from "@streamsy/core";
import {
  CommitHooks,
  ProjectionMarks,
  compileSegments,
  memoryProjectionMarks,
  ownerOf,
  type ProjectionMarksApi,
} from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import { inputEntries, type InputMap } from "./batch.ts";
import { isDeclaration, type DecodedParams, type Inputs } from "./declaration.ts";
import type { Executable } from "./executable.ts";
import { InvalidBinding, ProjectionFault } from "./fault.ts";
import { canonicalParams, withParams, type Unit } from "./unit.ts";
import { catchUp, progressOf, type RunContext } from "./run.ts";
import { bindingDelay } from "./binding-retry.ts";

import { Registry, RunnerEvent, eventPublisher, type RunnerFailure } from "./registry.ts";
import {
  hasNonRetryableHandler,
  nonRetryableHandlerReason,
  retryable,
  runnerFailure,
} from "./failure.ts";
import { triggersOf } from "./triggers.ts";
import { storageFault } from "./storage-fault.ts";
import type { WakeOutput } from "./wake.ts";
import { CheckpointStore } from "./checkpoint-store.ts";
import { createOutputs } from "./outputs.ts";
import { GenerationOutputs } from "./per-generation.ts";
import { tails } from "./catch-up.ts";

/** Re-read wall time after a suspended host resumes, even for distant deadlines. */
const MAX_DUE_WAIT_MS = 30_000;

const SCHEDULE_CHUNK = 100;

export const DEFAULT_LIMIT = 100;

/** Due marks that one drain pass adds to the in-memory set. */
export const DEFAULT_BATCH = 25;

const MAX_DEFER_MS = 60 * 60 * 1_000;

/** The wait before a retryable failed binding runs again: 1 s, doubling, at most 1 hour. */
export const deferDelay = (attempts: number): number =>
  Math.min(1_000 * 2 ** Math.max(0, attempts - 1), MAX_DEFER_MS);

export interface RunnerOptions<BE, BR> {
  readonly bindings?: Effect.Effect<ReadonlyArray<DecodedParams>, BE, BR>;
  readonly triggers?: ReadonlyArray<WakeOutput<{}>> | undefined;
  /** Units for each binding in one run. Default 100. */
  readonly limit?: number | undefined;
  /** Keep a pending mark for each binding in the storage owner. Default false. */
  readonly durable?: boolean | undefined;
  /** Due marks read in one drain pass. Default 25. */
  readonly batch?: number | undefined;
  /** Bindings run at once. Default 1. */
  readonly concurrency?: number | undefined;
}

/** The part of a projection declaration that the attach runner reads. */
export interface Attachable<B, HE = never, HR = never, Params = DecodedParams> {
  readonly retry?:
    | {
        readonly schedule?: Schedule.Schedule<unknown>;
        readonly maxAttempts?: number;
        // Method syntax retains the declaration's decoded param type at this erased boundary.
        onExhausted?(
          this: void,
          unit: Unit<unknown>,
          failure: RunnerFailure,
        ): Effect.Effect<void, HE, HR>;
      }
    | undefined;
  readonly id: string;
  readonly generation: number;
  readonly params: StreamRoute.ParamCodecs;
  readonly inputs: Inputs;
  // Method syntax: a declaration with typed params is assignable here.
  at(params: Params): B;
}

/**
 * The binding of a written stream id, if the projection reads that id.
 *
 * The rule: each input must give every projection param, so that one id picks
 * one binding. A declaration input with a template is checked here and throws a
 * RangeError. A custom route has no template, so an id whose parse misses a
 * param is skipped. A fixed address triggers only a projection with no params.
 */
export const matcher = <B extends { readonly params: Readonly<Record<string, string>> }>(
  declaration: Omit<Attachable<B>, "retry">,
): ((id: StreamId) => Option.Option<readonly [string, B]>) => {
  const names = Record.keys(declaration.params);
  const inputs = Record.toEntries(declaration.inputs);

  for (const [name, input] of inputs) {
    const given = isDeclaration(input)
      ? input.template.length === 0
        ? names
        : compileSegments(input.template).names
      : [];

    const missing = Array.filter(names, (param) => !given.includes(param));

    if (missing.length > 0)
      throw new RangeError(
        `Projection ${declaration.id} cannot attach: input ${name} does not give param ${Array.join(missing, ", ")}`,
      );
  }

  const bound = (params: Readonly<Record<string, unknown>>) => {
    const binding = declaration.at(params);

    return Option.some([canonicalParams(binding.params), binding] as const);
  };

  return (id) => {
    for (const [, input] of inputs) {
      // A fixed address only passes the rule above when the projection has no params.
      if (!isDeclaration(input)) {
        if (input.id === id) return bound({});
        continue;
      }

      const parsed = input.parse(id);

      if (Option.isNone(parsed)) continue;
      const params: Readonly<Record<string, unknown>> = parsed.value;

      if (Array.every(names, (name) => name in params)) return bound(params);
    }

    return Option.none();
  };
};

const ParamsJson = Schema.fromJsonString(Schema.Record(Schema.String, Schema.String));

/**
 * The binding of a pending mark's params, if they still decode with the
 * projection's param codecs and encode back to the same key.
 */
export const bindingOf = <B extends { readonly params: Readonly<Record<string, string>> }>(
  declaration: Omit<Attachable<B>, "retry">,
  key: string,
): Option.Option<B> =>
  Option.flatMap(Schema.decodeOption(ParamsJson)(key), (encoded) => {
    const decoded: Record<string, unknown> = {};

    for (const [name, codec] of Record.toEntries(declaration.params)) {
      const value = encoded[name];

      if (value === undefined) return Option.none();
      const result = Schema.decodeOption(codec)(value);

      if (Option.isNone(result)) return Option.none();
      decoded[name] = result.value;
    }

    try {
      const binding = declaration.at(decoded);

      return canonicalParams(binding.params) === key ? Option.some(binding) : Option.none();
    } catch {
      return Option.none();
    }
  });

const startupInventory = Effect.fn("Projection.startupInventory")(function* <
  B extends {
    readonly params: Readonly<Record<string, string>>;
  },
>(declaration: Omit<Attachable<B>, "retry">) {
  const store = yield* Effect.serviceOption(CheckpointStore);
  const list = Option.getOrUndefined(store)?.list;
  const id = declaration.id;

  const discover = Effect.gen(function* () {
    if (list === undefined) return [];

    const keys = new Set<string>();
    const current = new Set<string>();

    // Include older generations discovered in every stored version slot.
    for (const { generation, params } of yield* list(id))
      (generation === declaration.generation ? current : keys).add(canonicalParams(params));

    if (Record.isEmptyRecord(declaration.params)) keys.add(canonicalParams({}));

    for (const key of current) keys.delete(key);

    const candidates: B[] = [];

    for (const key of keys) {
      const bound = bindingOf(declaration, key);

      if (Option.isSome(bound)) candidates.push(bound.value);
      else
        yield* Effect.logWarning(
          `Projection ${id} skipped startup params ${key}: they no longer decode`,
        );
    }

    return candidates;
  });

  // One unreadable record must not stop the live commits of every binding of the id.
  return yield* discover.pipe(
    Effect.catchIf(
      (fault) => fault.reason === "unsupported-composition" || fault.reason === "invalid-record",
      (fault) =>
        Effect.logWarning(
          `Projection ${id} skipped startup inventory: ${fault.message}. Supply attach bindings explicitly.`,
        ).pipe(Effect.as([])),
    ),
  );
});

/**
 * A binding that faults with `history-unavailable` on an input whose head is
 * absent can never run: its source is gone, and an id never returns. With a
 * checkpoint, `readOrGone` turns this into a `gone` unit, so only a binding
 * with no checkpoint reaches here. Any other head answer keeps the fault.
 */
const sourceGone = <I extends InputMap, E>(inputs: I, cause: Cause.Cause<E | ProjectionFault>) =>
  Effect.gen(function* () {
    const fault = Cause.findErrorOption(cause).pipe(
      Option.filter(
        (error): error is ProjectionFault =>
          Schema.is(ProjectionFault)(error) && error.reason === "history-unavailable",
      ),
    );

    if (Option.isNone(fault) || fault.value.input === undefined) return false;
    const input = fault.value.input;
    const address = Array.findFirst(inputEntries(inputs), ([name]) => name === input);

    if (Option.isNone(address)) return false;
    const { wire } = yield* address.value[1].stream;

    return yield* wire.head(address.value[1].id).pipe(
      Effect.as(false),
      Effect.catchTags({
        StreamNotFound: () => Effect.succeed(true),
        StreamGone: () => Effect.succeed(true),
      }),
      Effect.orElseSucceed(() => false),
    );
  });

/**
 * Register the commit hook that marks each binding a transaction writes, in
 * that transaction. Dies when the storage layer has no marks, for example
 * over remote storage.
 */
const durableMarks = (
  projection: string,
  match: (id: StreamId) => Option.Option<readonly [string, unknown]>,
) =>
  Effect.gen(function* () {
    const hooks = yield* Effect.serviceOption(CommitHooks);
    const marks = yield* Effect.serviceOption(ProjectionMarks);

    if (Option.isNone(hooks) || Option.isNone(marks))
      return yield* Effect.die(
        new Error(
          `Projection ${projection} cannot attach durably: the storage layer keeps no projection marks. Use a local memory or SQL storage layer, or pass durable: false.`,
        ),
      );

    yield* hooks.value.beforeCommit((ids) => {
      const keys = new Set<string>();

      for (const id of ids) {
        const found = match(id);

        if (Option.isSome(found)) keys.add(found.value[0]);
      }

      return keys.size === 0 ? Effect.void : marks.value.mark(projection, [...keys]);
    });

    return marks.value;
  });

/**
 * One scoped commit subscription and pending set, shared by the two host
 * adapters. The subscription exists when this effect returns, so a commit
 * after that point always marks its binding.
 *
 * With `durable`, a commit hook also writes a pending mark for each binding in
 * the commit's transaction. The in-memory set stays the fast path. A binding
 * that catches up clears its mark with a `seq` compare, so a write during the
 * run keeps it. After a restart, `next`, `wait` and `drain` find the marks in
 * storage. Otherwise marks are private to this runner. In both modes a failed
 * binding backs off or stops its own mark, and its neighbours run. A start
 * makes stopped marks due once before answering next, wait or drain.
 */
export const runnerOf = <I extends InputMap, E, R, Q, Result, P, OR, HE, HR, BE, BR>(
  declaration: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>, HE, HR>,
  options: RunnerOptions<BE, BR> = {},
  defaultDurable = false,
  host: "process" | "wakeup" = "process",
) => {
  if ("retry" in options) throw new RangeError("Projection.attach retry was renamed to passRetry");

  const durable = options.durable ?? defaultDurable;
  const limit = options.limit ?? DEFAULT_LIMIT;
  const batch = options.batch ?? DEFAULT_BATCH;
  const concurrency = options.concurrency ?? 1;

  if (!Number.isSafeInteger(limit) || limit <= 0)
    throw new RangeError("Projection.attach limit must be a positive safe integer");

  if (!Number.isSafeInteger(batch) || batch <= 0)
    throw new RangeError("Projection.attach batch must be a positive safe integer");

  if (!Number.isSafeInteger(concurrency) || concurrency <= 0)
    throw new RangeError("Projection.attach concurrency must be a positive safe integer");
  const match = matcher(declaration);

  type Binding = Executable<I, E, R, Q, Result, P, unknown, OR>;

  return Effect.gen(function* () {
    const storage = yield* Storage;
    const owner = ownerOf(storage);
    const limitKey = Symbol.for("@streamsy/storage/MaxProjectionIdLength");
    const maxIdLength = limitKey in owner ? owner[limitKey] : undefined;

    // An invalid declaration fails at acquisition, like other attach configuration errors.
    if (Predicate.isNumber(maxIdLength) && Array.fromIterable(declaration.id).length > maxIdLength)
      return yield* Effect.die(
        new StorageFault({
          operation: "projection.attach",
          message: `MySQL projection id must be at most ${maxIdLength} characters`,
          retryable: false,
        }),
      );

    const registry = yield* Effect.serviceOption(Registry);
    const publish = Option.isSome(registry) ? eventPublisher(registry.value) : undefined;

    const committed = yield* storage.committed;
    const pending = new Map<string, Binding>();
    const working = new Set<string>();
    const wake = yield* Queue.sliding<void>(1);
    const signal = Queue.offer(wake, undefined).pipe(Effect.asVoid);
    // Normal waits retain the request until drain; backoff waits consume it and request a pass.
    const interruptedDelay = Queue.peek(wake).pipe(Effect.asVoid);
    const requested = Queue.take(wake).pipe(Effect.as(true));
    let active = 0;

    const runActive = (binding: Binding, context: RunContext) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          active += 1;
        }),
        () => {
          const running = catchUp(binding, { limit }, context).pipe(
            Effect.flatMap((cycle) => progressOf(binding, cycle)),
          );

          return publish === undefined
            ? running
            : running.pipe(
                Effect.tap((progress) =>
                  publish(
                    RunnerEvent.Ran({
                      projection: declaration.id,
                      params: binding.params,
                      status: progress.status,
                      units: progress.units,
                      offsets: progress.offsets,
                      wakeAt: progress.wakeAt,
                    }),
                  ),
                ),
              );
        },
        () =>
          Effect.sync(() => {
            active -= 1;
          }),
      );

    const marks: ProjectionMarksApi = durable
      ? yield* durableMarks(declaration.id, match)
      : memoryProjectionMarks();

    let failure = Option.none<RunnerFailure>();
    let started = false;
    const startPermit = yield* Semaphore.make(1);

    const prepare = Effect.fnUntraced(function* (binding: Binding) {
      const loaded = yield* binding.commit.load(binding);

      if (Option.isSome(loaded.record)) return false;

      if ((binding[GenerationOutputs]?.size ?? 0) > 0) {
        // Reading the heads fails with history-unavailable before outputs exist for a lost input.
        yield* tails(binding);
        yield* createOutputs(binding);
      }

      return true;
    });

    const seed = Effect.gen(function* () {
      const candidates =
        options.bindings === undefined
          ? yield* startupInventory(declaration)
          : yield* options.bindings.pipe(
              Effect.map(Array.map((params) => declaration.at(params))),
              // As with an unreadable inventory, live commits still run without startup replay.
              Effect.catch((error) =>
                Effect.logError(
                  `Projection ${declaration.id} skipped startup replay: the bindings option failed`,
                  error,
                ).pipe(Effect.as([])),
              ),
            );

      const byKey = new Map(
        Array.map(candidates, (binding) => [canonicalParams(binding.params), binding] as const),
      );

      for (const chunk of Array.chunksOf([...byKey], SCHEDULE_CHUNK)) {
        const keys: string[] = [];

        for (const [key, binding] of chunk) {
          const ready = yield* prepare(binding).pipe(
            Effect.catchIf(
              (fault) =>
                fault.reason === "retired-generation" || fault.reason === "history-unavailable",
              () => Effect.succeed(false),
            ),
          );

          if (ready) keys.push(key);
        }

        // A durable mark survives partial replay, even once a checkpoint exists.
        if (keys.length > 0)
          yield* marks.mark(declaration.id, keys).pipe(Effect.mapError(storageFault("marks.mark")));
      }
    }).pipe(
      Effect.mapError(
        (cause) =>
          new StorageFault({
            operation: "projection.startup",
            message: `Cannot queue startup replay for ${declaration.id}`,
            retryable: retryable(Cause.fail(cause)),
            cause,
          }),
      ),
    );

    // Retry failed startup work on the next request, without failing layer acquisition.
    const start = startPermit.withPermit(
      Effect.gen(function* () {
        if (started) return;
        yield* marks.resume(declaration.id);
        yield* seed;
        started = true;
      }),
    );

    const observeFailure = <A, EA, RA>(effect: Effect.Effect<A, EA, RA>) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            failure = Option.some(runnerFailure(cause, yield* Clock.currentTimeMillis));

            return yield* Effect.failCause(cause);
          }),
        ),
      );

    const triggers =
      options.triggers === undefined || options.triggers.length === 0
        ? undefined
        : yield* triggersOf(
            declaration.id,
            declaration.generation,
            options.triggers,
            match,
            marks,
            host,
          );

    const add = (ids: ReadonlyArray<StreamId>) =>
      Effect.gen(function* () {
        const bindings = new Map<string, Binding>();

        for (const id of ids) {
          const found = match(id);

          if (Option.isSome(found)) bindings.set(...found.value);
        }

        // Durable commits already marked in their transaction. Private marks live
        // only for this runner and are raised when its subscription reads the commit.
        if (!durable && bindings.size > 0)
          yield* marks
            .mark(declaration.id, [...bindings.keys()])
            .pipe(Effect.mapError(storageFault("marks.mark")));

        for (const [key, binding] of bindings) pending.set(key, binding);
      });

    const absorb = PubSub.takeUpTo(committed, Number.POSITIVE_INFINITY).pipe(
      Effect.flatMap((commits) => Effect.forEach(commits, add, { discard: true })),
    );

    const next = Effect.gen(function* () {
      yield* start;
      yield* absorb;

      if (
        (yield* Queue.size(wake)) > 0 ||
        pending.size > 0 ||
        (triggers !== undefined && (yield* triggers.next))
      )
        return Option.some(yield* Clock.currentTimeMillis);

      return yield* marks.nextDue(declaration.id);
    });

    const take = Effect.raceFirst(Effect.flatMap(PubSub.take(committed), add), interruptedDelay);

    const waitDue = Effect.gen(function* () {
      const due = yield* marks.nextDue(declaration.id);

      if (Option.isNone(due)) return yield* take;
      const delay = due.value - (yield* Clock.currentTimeMillis);

      // Wake on a commit or when the earliest deferred mark falls due.
      return yield* delay <= 0
        ? Effect.void
        : take.pipe(
            Effect.timeoutOrElse({
              duration: Math.min(delay, MAX_DUE_WAIT_MS),
              orElse: () => Effect.void,
            }),
          );
    });

    /** Wait for a commit or the next due mark. */
    const wait = Effect.gen(function* () {
      yield* start;
      yield* absorb;

      // A failed remote page gets another pass after the host's passRetry wait,
      // without making next report immediate work while that owner is unavailable.
      if (
        pending.size > 0 ||
        (triggers !== undefined && (triggers.retrying() || (yield* triggers.next)))
      )
        return undefined;

      return yield* waitDue;
    });

    const dropped = (key: string, binding: Binding) =>
      Effect.logWarning(
        `Projection ${declaration.id} dropped binding ${key}: its source was removed before its first run`,
      ).pipe(
        Effect.andThen(
          publish === undefined
            ? Effect.void
            : publish(
                RunnerEvent.Dropped({
                  projection: declaration.id,
                  params: binding.params,
                }),
              ),
        ),
      );

    /**
     * The in-memory set plus up to `batch` due marks. A binding that catches up
     * clears its mark by `seq`. A failed binding backs off or stops its own
     * mark without failing the pass.
     */
    const drainMarks = Effect.gen(function* () {
      const id = declaration.id;
      const now = yield* Clock.currentTimeMillis;
      const work = new Map(pending);
      pending.clear();

      for (const row of yield* marks.due(id, now, batch)) {
        if (work.has(row.params)) continue;
        const binding = bindingOf(declaration, row.params);

        if (Option.isNone(binding)) {
          yield* Effect.logWarning(
            `Projection ${id} removed pending mark ${row.params}: its params no longer decode`,
          );
          yield* marks.remove(id, row.params);

          continue;
        }

        work.set(row.params, binding.value);
      }

      for (const key of work.keys()) working.add(key);

      // Read before any run, so a write during a run raises `seq` past this value.
      const states = yield* marks.read(id, [...work.keys()]);

      // A commit records work without shortening a failed binding's backoff.
      // Its mark holds the work until due; keeping it pending would wake every pass.
      for (const [key, state] of states) {
        if (state.stopped || state.dueAt > now) {
          work.delete(key);
          working.delete(key);
        }
      }

      const runBinding = ([key, binding]: readonly [string, Binding]) =>
        Effect.gen(function* () {
          working.delete(key);
          const state = states.get(key);
          let unit: Unit | undefined;

          const exit = yield* Effect.exit(
            runActive(binding, {
              attempt: (state?.attempts ?? 0) + 1,
              unit: (formed) => {
                unit = formed;
              },
            }),
          );

          if (Exit.isSuccess(exit)) {
            if (exit.value.status === "limit-reached") pending.set(key, binding);
            else if (Option.isSome(exit.value.wakeAt))
              yield* marks.sleep(id, key, state?.seq ?? 0, exit.value.wakeAt.value);
            else if (state !== undefined) yield* marks.clear(id, key, state.seq);

            return;
          }

          if (yield* sourceGone(binding.inputs, exit.cause)) {
            yield* dropped(key, binding);
            yield* marks.remove(id, key);

            return;
          }

          const attempts = (state?.attempts ?? 0) + 1;
          const failed = runnerFailure(exit.cause, yield* Clock.currentTimeMillis);

          const budgetEnded =
            attempts >= (declaration.retry?.maxAttempts ?? Number.POSITIVE_INFINITY);

          const delay =
            !failed.retryable || budgetEnded
              ? Option.none<number>()
              : yield* bindingDelay(declaration.retry?.schedule, attempts);

          const exhausted = failed.retryable && Option.isNone(delay);

          if (exhausted && declaration.retry?.onExhausted !== undefined) {
            const hook = declaration.retry.onExhausted;
            const failedUnit = unit;

            if (failedUnit === undefined)
              yield* Effect.logWarning(
                `Projection ${id} binding ${key} exhausted; skipped onExhausted because no unit formed`,
              );
            else
              yield* Effect.suspend(() => {
                const decoded = Record.map(declaration.params, (codec, name) =>
                  Schema.decodeSync(codec)(binding.params[name]!),
                );

                return hook(withParams(failedUnit, decoded), failed);
              }).pipe(
                Effect.catchCause((cause) =>
                  Effect.logError(`Projection ${id} binding ${key} onExhausted failed`, cause),
                ),
              );
          }

          const dueAt = Option.map(delay, (ms) => failed.at + ms);

          const reason = hasNonRetryableHandler(exit.cause)
            ? Option.some(nonRetryableHandlerReason)
            : failed.reason;

          yield* marks.fail(id, key, { attempts, dueAt, failure: { ...failed, reason } });

          if (publish !== undefined)
            yield* publish(
              RunnerEvent.Failed({
                projection: id,
                params: binding.params,
                attempts,
                dueAt,
                failure: failed,
                stopped: Option.isNone(dueAt),
              }),
            );
          yield* Effect.logWarning(
            `Projection ${id} binding ${key} failed (attempt ${attempts}); ${Option.isSome(delay) ? `it runs again in ${delay.value} ms` : "stopped until retry or restart"}`,
            exit.cause,
          );
        });

      yield* Effect.forEach(work, runBinding, { concurrency, discard: true });

      yield* absorb;

      if (pending.size > 0) return { more: true };
      const after = yield* marks.nextDue(id);

      return { more: Option.isSome(after) && after.value <= (yield* Clock.currentTimeMillis) };
    });

    const drain = Effect.fn("Projection.drain")(function* () {
      yield* start;
      yield* Queue.clear(wake);
      yield* absorb;

      if (triggers !== undefined) yield* triggers.read;

      const result = yield* drainMarks;

      if (triggers !== undefined) yield* triggers.readRemote;

      failure = Option.none();

      return { more: result.more || (triggers !== undefined && (yield* triggers.next)) };
    });

    return {
      follow: (policy: Schedule.Schedule<unknown>) =>
        triggers?.follow(signal, policy) ?? Effect.void,
      next: observeFailure(next),
      wait: observeFailure(wait),
      resume: requested,
      /** True when a consumed request permits a pass without another wait. */
      interruptDelay: (duration: number) =>
        requested.pipe(Effect.timeoutOrElse({ duration, orElse: () => Effect.succeed(false) })),
      schedule: (bindings: ReadonlyArray<{ readonly params: Readonly<Record<string, string>> }>) =>
        Effect.gen(function* () {
          const validated = new Map<string, ReturnType<typeof declaration.at>>();

          for (const binding of bindings) {
            const key = canonicalParams(binding.params);
            const bound = bindingOf(declaration, key);

            if (Option.isNone(bound))
              return yield* new InvalidBinding({
                projection: declaration.id,
                params: binding.params,
              });
            validated.set(key, bound.value);
          }

          // Persist every chunk before exposing pending work. Earlier chunks may remain
          // marked if a later storage operation fails; repeating a request is safe.
          for (const keys of Array.chunksOf([...validated.keys()], SCHEDULE_CHUNK)) {
            yield* marks
              .mark(declaration.id, keys)
              .pipe(Effect.mapError(storageFault("marks.mark")));

            for (const key of keys) yield* marks.retry(declaration.id, key);
          }

          for (const [key, bound] of validated) pending.set(key, bound);
          yield* signal;

          return undefined;
        }).pipe(Effect.uninterruptible),
      rescan: (stream: string) =>
        Effect.gen(function* () {
          if (triggers === undefined)
            return yield* Effect.die(
              new RangeError(`Projection ${declaration.id} has no trigger ${stream}`),
            );
          yield* triggers.rescan(stream);
          yield* signal;

          return undefined;
        }).pipe(Effect.uninterruptible),
      retry: (binding?: { readonly params: Readonly<Record<string, string>> }) =>
        Effect.gen(function* () {
          const key = binding === undefined ? undefined : canonicalParams(binding.params);
          const count = yield* marks.retry(declaration.id, key);

          if (binding === undefined && triggers !== undefined) yield* triggers.retry();

          yield* signal;

          return count;
        }).pipe(Effect.uninterruptible),
      failed: (
        failureLimit: number,
        binding?: { readonly params: Readonly<Record<string, string>> },
      ) =>
        Effect.gen(function* () {
          const rows =
            binding === undefined
              ? yield* marks.failed(declaration.id, failureLimit)
              : Array.filter(
                  [
                    ...(yield* marks.read(declaration.id, [
                      canonicalParams(binding.params),
                    ])).values(),
                  ],
                  (row) => row.attempts > 0,
                );

          return yield* Effect.forEach(rows, (row) =>
            Effect.gen(function* () {
              const params = yield* Schema.decodeEffect(ParamsJson)(row.params).pipe(
                Effect.mapError(
                  (cause) =>
                    new StorageFault({
                      operation: "marks.failed",
                      retryable: false,
                      message: "Invalid stored binding params",
                      cause,
                    }),
                ),
              );

              return {
                params,
                attempts: row.attempts,
                dueAt: row.stopped ? Option.none<number>() : Option.some(row.dueAt),
                stopped: row.stopped,
                exhausted:
                  row.stopped &&
                  !Option.contains(
                    Option.flatMap(row.failure, (failed) => failed.reason),
                    nonRetryableHandlerReason,
                  ) &&
                  (row.attempts >= (declaration.retry?.maxAttempts ?? Number.POSITIVE_INFINITY) ||
                    Option.isNone(yield* bindingDelay(declaration.retry?.schedule, row.attempts))),
                failure: row.failure,
              };
            }),
          );
        }),
      drain: observeFailure(drain()).pipe(Effect.ensuring(Effect.sync(() => working.clear()))),
      status: Effect.gen(function* () {
        const summary = yield* marks.summary(declaration.id, yield* Clock.currentTimeMillis);

        return {
          active,
          queued: new Set([...pending.keys(), ...working]).size,
          marks: summary,
          failure,
          stopped: summary.stopped,
          triggers: triggers?.status() ?? [],
        };
      }),
      deferred: Effect.gen(function* () {
        const summary = yield* marks.summary(declaration.id, yield* Clock.currentTimeMillis);

        return summary.retryDue + summary.backingOff + summary.stopped;
      }),
    };
  });
};
