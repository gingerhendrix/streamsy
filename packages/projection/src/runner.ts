import { Array, Cause, Clock, Effect, Exit, Option, PubSub, Record, Schema } from "effect";
import { Storage, type StreamId } from "@streamsy/core";
import {
  CommitHooks,
  ProjectionMarks,
  compileSegments,
  type ProjectionMarksApi,
} from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import { inputEntries, type InputMap } from "./batch.ts";
import { isDeclaration, type Inputs } from "./declaration.ts";
import type { Executable } from "./executable.ts";
import { ProjectionFault } from "./fault.ts";
import { canonicalParams } from "./unit.ts";
import { run } from "./run.ts";

export const DEFAULT_LIMIT = 100;

/** Due marks that one durable drain pass adds to the in-memory set. */
export const DEFAULT_BATCH = 25;

const MAX_DEFER_MS = 60 * 60 * 1_000;

/** The wait before a failed binding runs again: 1 s, doubling, at most 1 hour. */
export const deferDelay = (attempts: number): number =>
  Math.min(1_000 * 2 ** Math.max(0, attempts - 1), MAX_DEFER_MS);

export interface RunnerOptions {
  /** Units for each binding in one run. Default 100. */
  readonly limit?: number | undefined;
  /** Keep a pending mark for each binding in the storage owner. Default false. */
  readonly durable?: boolean | undefined;
  /** Due marks read in one durable drain pass. Default 25. */
  readonly batch?: number | undefined;
}

/** The part of a projection declaration that the attach runner reads. */
export interface Attachable<B> {
  readonly id: string;
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
 * storage. A failed binding defers its own mark, and its neighbours run.
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
    const committed = yield* storage.committed;
    const pending = new Map<string, Binding>();

    const marks: ProjectionMarksApi | undefined =
      options.durable === true ? yield* durableMarks(declaration.id, match) : undefined;

    const add = (ids: ReadonlyArray<StreamId>) => {
      for (const id of ids) {
        const found = match(id);

        if (Option.isSome(found)) pending.set(found.value[0], found.value[1]);
      }
    };

    const absorb = Effect.map(
      PubSub.takeUpTo(committed, Number.POSITIVE_INFINITY),
      Array.forEach(add),
    );

    const next = Effect.gen(function* () {
      yield* absorb;

      if (pending.size > 0) return Option.some(yield* Clock.currentTimeMillis);

      return marks === undefined ? Option.none<number>() : yield* marks.nextDue(declaration.id);
    });

    const take = Effect.map(PubSub.take(committed), add);

    const waitDue = (store: ProjectionMarksApi) =>
      Effect.gen(function* () {
        const due = yield* store.nextDue(declaration.id);

        if (Option.isNone(due)) return yield* take;
        const delay = due.value - (yield* Clock.currentTimeMillis);

        // Wake on a commit or when the earliest deferred mark falls due.
        return yield* delay <= 0
          ? Effect.void
          : take.pipe(Effect.timeoutOrElse({ duration: delay, orElse: () => Effect.void }));
      });

    /** Wait for a commit, or in durable mode for the next due mark. */
    const wait = Effect.gen(function* () {
      yield* absorb;

      if (pending.size > 0) return undefined;

      return yield* marks === undefined ? take : waitDue(marks);
    });

    const dropped = (key: string) =>
      Effect.logWarning(
        `Projection ${declaration.id} dropped binding ${key}: its source was removed before its first run`,
      );

    /**
     * Every binding runs once, even if a peer fails; failed or bounded work
     * stays pending. A binding whose source went before its first run is
     * dropped, because no later run can succeed.
     */
    const drainInMemory = Effect.gen(function* () {
      let failure: Cause.Cause<E | ProjectionFault> | undefined;

      for (const [key, binding] of Array.fromIterable(pending)) {
        pending.delete(key);
        const exit = yield* Effect.exit(run(binding, { limit }));

        if (Exit.isSuccess(exit) && exit.value.status !== "limit-reached") continue;

        if (Exit.isFailure(exit) && (yield* sourceGone(binding.inputs, exit.cause))) {
          yield* dropped(key);

          continue;
        }

        pending.set(key, binding);

        if (Exit.isFailure(exit)) failure ??= exit.cause;
      }

      yield* absorb;

      if (failure !== undefined) return yield* Effect.failCause(failure);

      return { more: pending.size > 0 };
    });

    /**
     * The in-memory set plus up to `batch` due marks. A binding that catches up
     * clears its mark by `seq`. A failed binding defers its own mark and does
     * not fail the pass.
     */
    const drainDurable = (store: ProjectionMarksApi) =>
      Effect.gen(function* () {
        const id = declaration.id;
        const now = yield* Clock.currentTimeMillis;
        const work = new Map(pending);
        pending.clear();

        for (const row of yield* store.due(id, now, batch)) {
          if (work.has(row.params)) continue;
          const binding = bindingOf(declaration, row.params);

          if (Option.isNone(binding)) {
            yield* Effect.logWarning(
              `Projection ${id} removed pending mark ${row.params}: its params no longer decode`,
            );
            yield* store.remove(id, row.params);

            continue;
          }

          work.set(row.params, binding.value);
        }

        // Read before any run, so a write during a run raises `seq` past this value.
        const states = yield* store.read(id, [...work.keys()]);

        for (const [key, binding] of work) {
          const exit = yield* Effect.exit(run(binding, { limit }));
          const state = states.get(key);

          if (Exit.isSuccess(exit)) {
            if (exit.value.status === "limit-reached") pending.set(key, binding);
            else if (state !== undefined) yield* store.clear(id, key, state.seq);

            continue;
          }

          if (yield* sourceGone(binding.inputs, exit.cause)) {
            yield* dropped(key);
            yield* store.remove(id, key);

            continue;
          }

          const attempts = (state?.attempts ?? 0) + 1;
          const delay = deferDelay(attempts);
          yield* store.defer(id, key, attempts, (yield* Clock.currentTimeMillis) + delay);
          yield* Effect.logWarning(
            `Projection ${id} binding ${key} failed (attempt ${attempts}); it runs again in ${delay} ms`,
            exit.cause,
          );
        }

        yield* absorb;

        if (pending.size > 0) return { more: true };
        const after = yield* store.nextDue(id);

        return { more: Option.isSome(after) && after.value <= (yield* Clock.currentTimeMillis) };
      });

    const drain = Effect.fn("Projection.drain")(function* () {
      yield* absorb;

      return yield* marks === undefined ? drainInMemory : drainDurable(marks);
    });

    return {
      next,
      wait,
      drain: drain(),
      deferred: marks === undefined ? Effect.succeed(0) : marks.deferred(declaration.id),
    };
  });
};
