import {
  Array,
  Cause,
  Clock,
  Effect,
  Exit,
  Option,
  PubSub,
  Queue,
  Record,
  Schema,
  Semaphore,
} from "effect";
import { Storage, StorageFault, type StreamId } from "@streamsy/core";
import {
  CommitHooks,
  ProjectionMarks,
  compileSegments,
  memoryProjectionMarks,
  type ProjectionMarksApi,
} from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import { inputEntries, type InputMap } from "./batch.ts";
import { isDeclaration, type Inputs } from "./declaration.ts";
import type { Executable } from "./executable.ts";
import { InvalidBinding, ProjectionFault } from "./fault.ts";
import { canonicalParams } from "./unit.ts";
import { run } from "./run.ts";
import { Registry, RunnerEvent, eventPublisher, type RunnerFailure } from "./registry.ts";
import { runnerFailure } from "./failure.ts";
import { triggersOf } from "./triggers.ts";
import { storageFault } from "./storage-fault.ts";
import type { WakeOutput } from "./wake.ts";

export const DEFAULT_LIMIT = 100;

/** Due marks that one drain pass adds to the in-memory set. */
export const DEFAULT_BATCH = 25;

const MAX_DEFER_MS = 60 * 60 * 1_000;

/** The wait before a retryable failed binding runs again: 1 s, doubling, at most 1 hour. */
export const deferDelay = (attempts: number): number =>
  Math.min(1_000 * 2 ** Math.max(0, attempts - 1), MAX_DEFER_MS);

export interface RunnerOptions {
  readonly triggers?: ReadonlyArray<WakeOutput<{}>> | undefined;
  /** Units for each binding in one run. Default 100. */
  readonly limit?: number | undefined;
  /** Keep a pending mark for each binding in the storage owner. Default false. */
  readonly durable?: boolean | undefined;
  /** Due marks read in one drain pass. Default 25. */
  readonly batch?: number | undefined;
}

/** The part of a projection declaration that the attach runner reads. */
export interface Attachable<B> {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: StreamRoute.ParamCodecs;
  readonly inputs: Inputs;
  // Method syntax: a declaration with typed params is assignable here.
  at(params: Readonly<Record<string, unknown>>): B;
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
  declaration: Attachable<B>,
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
  declaration: Attachable<B>,
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
export const runnerOf = <I extends InputMap, E, R, Q, Result, P, OR>(
  declaration: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>>,
  options: RunnerOptions = {},
) => {
  const limit = options.limit ?? DEFAULT_LIMIT;
  const batch = options.batch ?? DEFAULT_BATCH;

  if (!Number.isSafeInteger(limit) || limit <= 0)
    throw new RangeError("Projection.attach limit must be a positive safe integer");

  if (!Number.isSafeInteger(batch) || batch <= 0)
    throw new RangeError("Projection.attach batch must be a positive safe integer");
  const match = matcher(declaration);

  type Binding = Executable<I, E, R, Q, Result, P, unknown, OR>;

  return Effect.gen(function* () {
    const storage = yield* Storage;
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

    const runActive = (binding: Binding) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          active += 1;
        }),
        () => {
          const running = run(binding, { limit });

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

    const marks: ProjectionMarksApi =
      options.durable === true
        ? yield* durableMarks(declaration.id, match)
        : memoryProjectionMarks();

    let failure = Option.none<RunnerFailure>();
    let started = false;
    const startPermit = yield* Semaphore.make(1);

    // Retry a failed resume on the next request, without failing layer acquisition.
    const start = startPermit.withPermit(
      Effect.gen(function* () {
        if (started) return;
        yield* marks.resume(declaration.id);
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
        : yield* triggersOf(declaration.id, options.triggers, match, marks);

    const add = (ids: ReadonlyArray<StreamId>) =>
      Effect.gen(function* () {
        const bindings = new Map<string, Binding>();

        for (const id of ids) {
          const found = match(id);

          if (Option.isSome(found)) bindings.set(...found.value);
        }

        // Durable commits already marked in their transaction. Private marks live
        // only for this runner and are raised when its subscription reads the commit.
        if (options.durable !== true && bindings.size > 0)
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
        : take.pipe(Effect.timeoutOrElse({ duration: delay, orElse: () => Effect.void }));
    });

    /** Wait for a commit or the next due mark. */
    const wait = Effect.gen(function* () {
      yield* start;
      yield* absorb;

      if (pending.size > 0 || (triggers !== undefined && (yield* triggers.next))) return undefined;

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

      for (const [key, binding] of work) {
        working.delete(key);
        const exit = yield* Effect.exit(runActive(binding));
        const state = states.get(key);

        if (Exit.isSuccess(exit)) {
          if (exit.value.status === "limit-reached") pending.set(key, binding);
          else if (state !== undefined) yield* marks.clear(id, key, state.seq);

          continue;
        }

        if (yield* sourceGone(binding.inputs, exit.cause)) {
          yield* dropped(key, binding);
          yield* marks.remove(id, key);

          continue;
        }

        const attempts = (state?.attempts ?? 0) + 1;
        const failed = runnerFailure(exit.cause, yield* Clock.currentTimeMillis);

        const dueAt = failed.retryable
          ? Option.some(failed.at + deferDelay(attempts))
          : Option.none<number>();

        yield* marks.fail(id, key, { attempts, dueAt, failure: failed });

        if (publish !== undefined)
          yield* publish(
            RunnerEvent.Failed({
              projection: id,
              params: binding.params,
              attempts,
              dueAt,
              failure: failed,
              stopped: !failed.retryable,
            }),
          );
        yield* Effect.logWarning(
          `Projection ${id} binding ${key} failed (attempt ${attempts}); ${failed.retryable ? `it runs again in ${deferDelay(attempts)} ms` : "stopped until retry or restart"}`,
          exit.cause,
        );
      }

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

      failure = Option.none();

      return { more: result.more || (triggers !== undefined && (yield* triggers.next)) };
    });

    return {
      next: observeFailure(next),
      wait: observeFailure(wait),
      resume: requested,
      /** True when a consumed request permits a pass without another wait. */
      interruptDelay: (duration: number) =>
        requested.pipe(Effect.timeoutOrElse({ duration, orElse: () => Effect.succeed(false) })),
      schedule: (binding: { readonly params: Readonly<Record<string, string>> }) =>
        Effect.gen(function* () {
          const key = canonicalParams(binding.params);
          const bound = bindingOf(declaration, key);

          if (Option.isNone(bound))
            return yield* new InvalidBinding({
              projection: declaration.id,
              params: binding.params,
            });

          // Persist before exposing pending work, so a fault cannot enqueue an unrecorded request.
          yield* marks
            .mark(declaration.id, [key])
            .pipe(Effect.mapError(storageFault("marks.mark")));
          yield* marks.retry(declaration.id, key);

          pending.set(key, bound.value);
          yield* signal;

          return undefined;
        }).pipe(Effect.uninterruptible),
      retry: (binding?: { readonly params: Readonly<Record<string, string>> }) =>
        Effect.gen(function* () {
          const key = binding === undefined ? undefined : canonicalParams(binding.params);
          const count = yield* marks.retry(declaration.id, key);

          if (binding === undefined) triggers?.retry();

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
