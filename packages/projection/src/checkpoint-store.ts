import { Array, Context, Effect, Layer, Option, Record, Schema, Semaphore } from "effect";
import { CommitBoundary, ZERO_OFFSET, type StorageFault } from "@streamsy/core";
import { OwnerRecords, attachOwner, ownerOf } from "@streamsy/core/internal";
import type { InputMap, Slice } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import { Counter, PendingUnit, canonicalParams, encodeKey, type Range } from "./unit.ts";

export const CheckpointRecord = Schema.Struct({
  identity: Schema.Struct({ inputs: Schema.Record(Schema.String, Schema.String) }),
  /** Input key to accepted offset; the next pass reads after these. */
  inputs: Schema.Record(Schema.String, Schema.String),
  pending: Schema.optionalKey(PendingUnit),
  state: Schema.optionalKey(Schema.String),
  adapters: Schema.Struct({
    outputs: Schema.optionalKey(
      Schema.Record(Schema.String, Schema.Struct({ epoch: Counter, nextSeq: Counter })),
    ),
  }),
});

export interface CheckpointRecord extends Schema.Schema.Type<typeof CheckpointRecord> {}

/** Stored value. The version is the compare-and-set token exposed as a string. */
const Envelope = Schema.Struct({ version: Counter, record: CheckpointRecord });

const EnvelopeJson = Schema.fromJsonString(Envelope);

export interface ProjectionKey {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
}

export const recordKey = (key: ProjectionKey): string =>
  encodeKey([
    "streamsy.projection.v1",
    key.id,
    String(key.version),
    String(key.generation),
    canonicalParams(key.params),
  ]);

/** Token "0" means no record is stored yet. */
const ABSENT_TOKEN = "0";

export interface Loaded {
  readonly record: Option.Option<CheckpointRecord>;
  readonly token: string;
}

export interface CheckpointStoreApi {
  readonly load: (key: ProjectionKey) => Effect.Effect<Loaded, ProjectionFault>;
  /** Compare-and-set inside the owner transaction; returns the new token. */
  readonly compareAndSet: (
    key: ProjectionKey,
    ifToken: string,
    record: CheckpointRecord,
  ) => Effect.Effect<string, ProjectionFault>;
  /** Deletes the record. Absent keys are ignored. */
  readonly remove: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
}

const storageFailure =
  (phase: ProjectionFault["phase"], message: string) => (cause: StorageFault) =>
    new ProjectionFault({ phase, reason: "storage-failure", message, cause });

/**
 * SQL storage fails a write with this operation when the transaction read the key as
 * absent and a concurrent save created it first. That save won the compare-and-set.
 */
const OWNER_RECORD_CONFLICT = "records.conflict";

/** Stores each projection's offsets and encoded state in one compare-and-set record. */
export class CheckpointStore extends Context.Service<CheckpointStore, CheckpointStoreApi>()(
  "@streamsy/projection/CheckpointStore",
) {
  /**
   * Checkpoints on the storage owner, in its transaction. Works over every Streamsy
   * storage host, so checkpoints and streams always share one owner.
   */
  static readonly layer: Layer.Layer<CheckpointStore, never, CommitBoundary | OwnerRecords> =
    Layer.effect(
      this,
      Effect.gen(function* () {
        const boundary = yield* CommitBoundary;
        const records = yield* OwnerRecords;

        const checkpoints = fromStore({
          read: (key) =>
            records
              .read(key)
              .pipe(Effect.mapError(storageFailure("load", "Cannot read a checkpoint"))),
          write: (key, value) =>
            records.write(key, value).pipe(
              Effect.mapError((cause) =>
                cause.operation === OWNER_RECORD_CONFLICT
                  ? new ProjectionFault({
                      phase: "checkpoint",
                      reason: "token-conflict",
                      message:
                        "A second writer on the same key likely created the checkpoint first. Use one writer per key: the follower or one run at a time.",
                      cause,
                    })
                  : storageFailure("checkpoint", "Cannot write a checkpoint")(cause),
              ),
            ),
          remove: (key) =>
            records
              .remove(key)
              .pipe(Effect.mapError(storageFailure("checkpoint", "Cannot delete a checkpoint"))),
          withTransaction: (body) =>
            boundary.withTransaction(body).pipe(
              Effect.catchTag("SqlError", (cause) =>
                Effect.fail(
                  new ProjectionFault({
                    phase: "checkpoint",
                    reason: "storage-failure",
                    message: "The owner transaction failed",
                    cause,
                  }),
                ),
              ),
            ),
        });

        attachOwner(checkpoints.load, ownerOf(boundary));

        return checkpoints;
      }),
    );
  /** An independent store for rebuilt values and tests. It shares no owner with streams. */
  static readonly layerMemory = Layer.effect(
    this,
    Effect.gen(function* () {
      const records = new Map<string, string>();
      const lock = yield* Semaphore.make(1);

      return fromStore({
        read: (key) => Effect.sync(() => Option.fromUndefinedOr(records.get(key))),
        write: (key, value) =>
          Effect.sync(() => {
            records.set(key, value);
          }),
        remove: (key) =>
          lock.withPermits(1)(
            Effect.sync(() => {
              records.delete(key);
            }),
          ),
        withTransaction: lock.withPermits(1),
      });
    }),
  );
}

/** The encoded seam a checkpoint Layer builds on; both shipped Layers use it. */
export interface EncodedStore {
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  readonly write: (key: string, value: string) => Effect.Effect<void, ProjectionFault>;
  readonly remove: (key: string) => Effect.Effect<void, ProjectionFault>;
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectionFault, R>;
}

export const fromStore = (store: EncodedStore): CheckpointStoreApi => {
  const load = Effect.fn("Projection.CheckpointStore.load")(function* (key: ProjectionKey) {
    const encoded = yield* store.read(recordKey(key));

    if (Option.isNone(encoded)) {
      const absent: Loaded = { record: Option.none(), token: ABSENT_TOKEN };

      return absent;
    }

    const envelope = yield* Schema.decodeEffect(EnvelopeJson)(encoded.value).pipe(
      Effect.mapError(
        () =>
          new ProjectionFault({
            phase: "load",
            reason: "invalid-record",
            message: `Invalid stored record for ${key.id}`,
          }),
      ),
    );

    const loaded: Loaded = {
      record: Option.some(envelope.record),
      token: String(envelope.version),
    };

    return loaded;
  });

  const compareAndSet = Effect.fn("Projection.CheckpointStore.compareAndSet")(function* (
    key: ProjectionKey,
    ifToken: string,
    record: CheckpointRecord,
  ) {
    return yield* store.withTransaction(
      Effect.gen(function* () {
        const current = yield* load(key);

        if (current.token !== ifToken)
          return yield* new ProjectionFault({
            phase: "checkpoint",
            reason: "token-conflict",
            message: `Record for ${key.id} changed: expected token ${ifToken}, found ${current.token}. A second writer on the same key is the likely cause. Use one writer per key: the follower or one run at a time.`,
          });
        const version = Number(ifToken) + 1;

        const encoded = yield* Schema.encodeEffect(EnvelopeJson)({ version, record }).pipe(
          Effect.mapError(
            () =>
              new ProjectionFault({
                phase: "checkpoint",
                reason: "invalid-record",
                message: `Cannot encode record for ${key.id}`,
              }),
          ),
        );

        yield* store.write(recordKey(key), encoded);

        return String(version);
      }),
    );
  });

  return {
    load,
    compareAndSet,
    remove: (key) => store.remove(recordKey(key)),
  };
};

/** What the kernel needs to locate a record; every projection value satisfies it. */
export interface Identity {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: InputMap;
}

const identityOf = (inputs: InputMap): Record<string, string> =>
  Record.fromEntries(
    Array.map(Record.toEntries(inputs), ([name, address]) => [
      name,
      encodeKey([address.id, address.contentType]),
    ]),
  );

const sameIdentity = (a: Record<string, string>, b: Record<string, string>): boolean => {
  const keys = Record.keys(a);

  return keys.length === Record.keys(b).length && Array.every(keys, (k) => a[k] === b[k]);
};

/** The stored record, or the unstored initial record for a fresh key. */
export const restore = Effect.fn("Projection.restore")(function* <R>(
  projection: Identity,
  owner: { readonly load: (key: ProjectionKey) => Effect.Effect<Loaded, ProjectionFault, R> },
) {
  const loaded = yield* owner.load(projection);
  const identity = { inputs: identityOf(projection.inputs) };

  if (Option.isNone(loaded.record)) {
    const record: CheckpointRecord = {
      identity,
      inputs: Record.fromEntries(
        Array.map(Record.keys(projection.inputs), (name) => [name, ZERO_OFFSET]),
      ),
      adapters: {},
    };

    return { record, token: loaded.token, loaded };
  }

  if (!sameIdentity(loaded.record.value.identity.inputs, identity.inputs))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "identity-mismatch",
      message: `Stored inputs differ from the declaration of ${projection.id}`,
    });

  for (const name of Record.keys(projection.inputs)) {
    if (loaded.record.value.inputs[name] === undefined)
      return yield* new ProjectionFault({
        phase: "load",
        reason: "invalid-record",
        input: name,
        message: `Stored record of ${projection.id} has no offset for ${name}`,
      });
  }

  return { record: loaded.record.value, token: loaded.token, loaded };
});

/** Accepted offsets after a unit: every read input moves to its `nextOffset`. */
export const advance = (
  inputs: Record<string, string>,
  ranges: Record<string, { readonly nextOffset: string }>,
): Record<string, string> =>
  Record.fromEntries(
    Array.map(Record.toEntries(inputs), ([name, offset]) => [
      name,
      ranges[name]?.nextOffset ?? offset,
    ]),
  );

/** Only the inputs that contributed items; the unit key and any pin come from these. */
export const rangesOf = (slices: Record<string, Slice<unknown>>): Record<string, Range> =>
  Record.fromEntries(
    Array.map(
      Array.filter(Record.toEntries(slices), ([, slice]) => slice.items.length > 0),
      ([name, slice]) => [name, { from: slice.from, nextOffset: slice.nextOffset }],
    ),
  );
