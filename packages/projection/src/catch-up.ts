import { Array, Effect, Option, Queue, Record, Schedule, Schema, Stream } from "effect";
import { Offset, StreamId, compare, type Streamsy } from "@streamsy/core";
import { inputEntries, type InputMap, type InputServices } from "./batch.ts";
import { restore, type Identity } from "./checkpoint-store.ts";
import { ProjectionFault } from "./fault.ts";
import { Registry } from "./registry.ts";
import type { Readable } from "./snapshot.ts";

const Tail = Schema.Struct({ stream: StreamId, offset: Offset, closed: Schema.Boolean });

/** A serializable vector of fixed source tails, keyed by input name. */
export const Tails = Schema.Record(Schema.String, Tail);

export type Tails<Inputs extends InputMap = InputMap> = {
  readonly [Name in keyof Inputs]: typeof Tail.Type;
};

export interface InputLag {
  readonly committed: Offset;
  readonly target: Offset;
  /** True while the input needs an offset, a close, or settlement of a pinned range. */
  readonly behind: boolean;
}

export interface Lag<Inputs extends InputMap = InputMap> {
  /** True when all source targets are committed and there is no pending unit. */
  readonly caughtUp: boolean;
  readonly inputs: { readonly [Name in keyof Inputs]: InputLag };
}

type Sources<Inputs extends InputMap> = Pick<Identity, "id"> & { readonly inputs: Inputs };

type Checkpointed<Inputs extends InputMap, Q> = Omit<Readable<Inputs, never, Q>, "state">;

const isOffset = Schema.is(Offset);

const historyUnavailable = (input: string, message: string, cause?: unknown) =>
  new ProjectionFault({ phase: "read", reason: "history-unavailable", input, message, cause });

const head = Effect.fn("Projection.catchUp.head")(function* <R>(
  name: string,
  address: Streamsy.Address<unknown, R>,
): Effect.fn.Return<typeof Tail.Type, ProjectionFault, R> {
  const { wire } = yield* address.stream;

  const storageFailure = (cause: unknown) =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input: name,
      message: `Cannot read the head of ${address.id}`,
      cause,
    });

  const result = yield* wire.head(address.id).pipe(
    Effect.catchTags({
      StreamNotFound: (cause) =>
        historyUnavailable(name, `Input stream ${address.id} is missing`, cause),
      StreamGone: (cause) => historyUnavailable(name, `Input stream ${address.id} is gone`, cause),
      StorageFault: storageFailure,
      TransportFault: storageFailure,
    }),
  );

  if (!isOffset(result.nextOffset))
    return yield* new ProjectionFault({
      phase: "read",
      reason: "invalid-source",
      input: name,
      message: `Head of ${address.id} has an invalid offset`,
    });

  return { stream: address.id, offset: result.nextOffset, closed: result.closed };
});

/** Capture each resolved input head once. The vector can be reused while producers keep writing. */
export const tails = Effect.fn("Projection.tails")(function* <Inputs extends InputMap>(
  projection: Sources<Inputs>,
): Effect.fn.Return<Tails<Inputs>, ProjectionFault, InputServices<Inputs>> {
  const entries = yield* Effect.forEach(inputEntries(projection.inputs), ([name, address]) =>
    head(name, address).pipe(Effect.map((value) => [name, value] as const)),
  );

  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: Each resolved input contributes exactly one entry.
  return Record.fromEntries(entries) as Tails<Inputs>;
});

const validateTails = Effect.fnUntraced(function* (projection: Sources<InputMap>, targets: Tails) {
  const decoded = yield* Schema.decodeEffect(Tails, {
    onExcessProperty: "error",
    reportInput: true,
  })(targets).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase: "load",
          reason: "invalid-options",
          message: `Invalid tails for ${projection.id}: ${cause.message}`,
          cause,
        }),
    ),
  );

  for (const [name, address] of inputEntries(projection.inputs)) {
    if (decoded[name]?.stream !== address.id)
      return yield* new ProjectionFault({
        phase: "load",
        reason: "identity-mismatch",
        input: name,
        message: `Tail for ${name} must name stream ${address.id}`,
      });
  }

  if (Record.size(decoded) !== Record.size(projection.inputs))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "identity-mismatch",
      message: `Tail input names differ from the declaration of ${projection.id}`,
    });

  return decoded;
});

const measure = Effect.fnUntraced(function* <Inputs extends InputMap, Q>(
  projection: Checkpointed<Inputs, Q>,
  targets: Tails,
  verifyHeads: boolean,
): Effect.fn.Return<Lag<Inputs>, ProjectionFault, Q | InputServices<Inputs>> {
  const { record } = yield* restore(projection, projection.commit);

  if (record.ended)
    return yield* new ProjectionFault({
      phase: "load",
      reason: "history-unavailable",
      message: `Binding of ${projection.id} ended after an input was lost`,
    });

  const entries = yield* Effect.forEach(
    inputEntries(projection.inputs),
    Effect.fnUntraced(function* ([name, address]) {
      const committed = record.inputs[name];
      const target = targets[name];

      if (!isOffset(committed))
        return yield* new ProjectionFault({
          phase: "load",
          reason: "invalid-record",
          input: name,
          message: `Stored offset for ${name} is not a valid offset`,
        });

      if (target === undefined)
        return yield* new ProjectionFault({
          phase: "load",
          reason: "identity-mismatch",
          input: name,
          message: `Missing tail for ${name}`,
        });

      if (verifyHeads) {
        const current = yield* head(name, address);
        const order = compare(current.offset, target.offset);

        if (order < 0 || (target.closed && (!current.closed || order !== 0)))
          return yield* historyUnavailable(
            name,
            `Head of ${address.id} no longer contains its tail`,
          );
      }

      const behind =
        compare(committed, target.offset) < 0 ||
        (target.closed && record.closed?.[name] !== true) ||
        (record.pending !== undefined && Record.has(record.pending.ranges, name));

      return [name, { committed, target: target.offset, behind }] as const;
    }),
  );

  return {
    caughtUp: record.pending === undefined && Array.every(entries, ([, input]) => !input.behind),
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: Each resolved input contributes exactly one entry.
    inputs: Record.fromEntries(entries) as Lag<Inputs>["inputs"],
  };
});

/** Compare fixed source tails with one committed checkpoint, including processed closes. Checks source readiness only. */
export const lag = Effect.fn("Projection.lag")(function* <Inputs extends InputMap, Q>(
  projection: Checkpointed<Inputs, Q>,
  targets?: Tails,
): Effect.fn.Return<Lag<Inputs>, ProjectionFault, Q | InputServices<Inputs>> {
  const fixed =
    targets === undefined ? yield* tails(projection) : yield* validateTails(projection, targets);

  return yield* measure(projection, fixed, targets !== undefined);
});

const pollInterval = "250 millis";

/** Wait for one fixed vector of source tails. The caller controls the timeout. */
export const caughtUp = Effect.fn("Projection.caughtUp")(function* <Inputs extends InputMap, Q>(
  projection: Checkpointed<Inputs, Q>,
  targets?: Tails,
): Effect.fn.Return<void, ProjectionFault, Q | InputServices<Inputs>> {
  const registry = yield* Effect.serviceOption(Registry);

  const wakeups = yield* Option.match(registry, {
    onNone: () => Effect.void,
    onSome: Effect.fnUntraced(function* (service) {
      // Acquire the subscription before capturing heads or loading the checkpoint.
      const pull = yield* Stream.toPull(service.events);
      const queue = yield* Queue.sliding<void>(1);

      yield* Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.runForEach(() => Queue.offer(queue, undefined)),
        Effect.forkScoped,
      );

      return queue;
    }),
  });

  const fixed =
    targets === undefined ? yield* tails(projection) : yield* validateTails(projection, targets);

  const check = measure(projection, fixed, true);

  if (wakeups === undefined)
    return yield* check.pipe(
      Effect.repeat({
        schedule: Schedule.spaced(pollInterval),
        while: (progress) => !progress.caughtUp,
      }),
      Effect.asVoid,
    );

  const wake = Queue.take(wakeups).pipe(
    Effect.timeoutOrElse({ duration: pollInterval, orElse: () => Effect.void }),
  );

  return yield* check.pipe(
    Effect.repeat({
      while: (progress) => (progress.caughtUp ? Effect.succeed(false) : Effect.as(wake, true)),
    }),
    Effect.asVoid,
  );
}, Effect.scoped);
