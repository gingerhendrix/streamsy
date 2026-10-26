import { Array, Cause, Clock, Effect, Exit, Option, PubSub, Record, Schema } from "effect";
import { Storage, type StreamId } from "@streamsy/core";
import { compileSegments } from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import { inputEntries, type InputMap } from "./batch.ts";
import { isDeclaration, type Inputs } from "./declaration.ts";
import type { Executable } from "./executable.ts";
import { ProjectionFault } from "./fault.ts";
import { canonicalParams } from "./unit.ts";
import { run } from "./run.ts";

export const DEFAULT_LIMIT = 100;

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
 * One scoped commit subscription and pending set, shared by the two host
 * adapters. The subscription exists when this effect returns, so a commit
 * after that point always marks its binding.
 */
export const runnerOf = <I extends InputMap, E, R, Q, Result, P, OR>(
  declaration: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>>,
  limit = DEFAULT_LIMIT,
) => {
  if (!Number.isSafeInteger(limit) || limit <= 0)
    throw new RangeError("Projection.attach limit must be a positive safe integer");
  const match = matcher(declaration);

  return Effect.gen(function* () {
    const storage = yield* Storage;
    const committed = yield* storage.committed;
    const pending = new Map<string, Executable<I, E, R, Q, Result, P, unknown, OR>>();

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

      return pending.size > 0 ? Option.some(yield* Clock.currentTimeMillis) : Option.none<number>();
    });

    const wait = Effect.gen(function* () {
      yield* absorb;

      if (pending.size === 0) add(yield* PubSub.take(committed));
    });

    /**
     * Every binding runs once, even if a peer fails; failed or bounded work
     * stays pending. A binding whose source went before its first run is
     * dropped, because no later run can succeed.
     */
    const drain = Effect.fn("Projection.drain")(function* () {
      yield* absorb;
      let failure: Cause.Cause<E | ProjectionFault> | undefined;

      for (const [key, binding] of Array.fromIterable(pending)) {
        pending.delete(key);
        const exit = yield* Effect.exit(run(binding, { limit }));

        if (Exit.isSuccess(exit) && exit.value.status !== "limit-reached") continue;

        if (Exit.isFailure(exit) && (yield* sourceGone(binding.inputs, exit.cause))) {
          yield* Effect.logWarning(
            `Projection ${declaration.id} dropped binding ${key}: its source was removed before its first run`,
          );

          continue;
        }

        pending.set(key, binding);

        if (Exit.isFailure(exit)) failure ??= exit.cause;
      }

      yield* absorb;

      if (failure !== undefined) return yield* Effect.failCause(failure);

      return { more: pending.size > 0 };
    });

    return { next, wait, drain: drain() };
  });
};
