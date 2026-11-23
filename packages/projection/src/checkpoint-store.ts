import { Array, Context, Effect, Layer, Option, Record, Schema, Semaphore } from "effect";
import { CommitBoundary, ZERO_OFFSET, type StorageFault } from "@streamsy/core";
import {
  OWNER_RECORDS_LIST_UNSUPPORTED,
  OwnerRecords,
  attachOwner,
  ownerOf,
} from "@streamsy/core/internal";
import type { InputMap, Slice } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import {
  Counter,
  PendingUnit,
  canonicalParams,
  encodeKey,
  terminalOf,
  type Range,
} from "./unit.ts";

export const CheckpointRecord = Schema.Struct({
  identity: Schema.Struct({ inputs: Schema.Record(Schema.String, Schema.String) }),
  /** Input key to accepted offset; the next pass reads after these. */
  inputs: Schema.Record(Schema.String, Schema.String),
  pending: Schema.optionalKey(PendingUnit),
  /** Next process wake, in epoch milliseconds. Absent means no timer. */
  wakeAt: Schema.optionalKey(Counter),
  /** Inputs whose terminal close has already been processed. */
  closed: Schema.optionalKey(Schema.Record(Schema.String, Schema.Boolean)),
  /** The final gone unit committed; only forgetting remains. */
  ended: Schema.optionalKey(Schema.Literal(true)),
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

const Tombstone = Schema.Struct({ version: Counter, retired: Schema.Struct({}) });

const TombstoneJson = Schema.fromJsonString(Tombstone);

const StoredJson = Schema.fromJsonString(Schema.Union([Envelope, Tombstone]));

export type RetirementStatus = "retired" | "already-retired";

export interface ProjectionKey {
  readonly id: string;
  readonly generation: number;
  readonly params: Record<string, string>;
}

const CHECKPOINT_NAMESPACE = "streamsy.projection.v1";

// Keep the former slot at "1" to preserve the existing default checkpoint and routed keys.
const STORED_VERSION_SLOT = "1";

const CheckpointKeyJson = Schema.fromJsonString(
  Schema.Tuple([
    Schema.Literal(CHECKPOINT_NAMESPACE),
    Schema.String, // id
    Schema.String, // legacy version slot, including values other than "1"
    Schema.FiniteFromString, // generation
    Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)),
  ]),
);

export const recordKey = (key: ProjectionKey): string =>
  encodeKey([
    CHECKPOINT_NAMESPACE,
    key.id,
    STORED_VERSION_SLOT,
    String(key.generation),
    canonicalParams(key.params),
  ]);

/** Token "0" means no record is stored yet. */
const ABSENT_TOKEN = "0";

export interface Loaded {
  readonly record: Option.Option<CheckpointRecord>;
  readonly token: string;
}

/** A binding found by `CheckpointStoreApi.list`. */
export interface ListedBinding {
  readonly generation: number;
  readonly params: Record<string, string>;
}

export interface RoutedStreamRecord {
  readonly output: string;
  readonly streamId: string;
}

/** Access the route inventory in the checkpoint owner's transaction. */
export interface RouteRecords {
  /** Whether this output owns the stream, including after a forget. */
  readonly has: (
    key: ProjectionKey,
    output: string,
    streamId: string,
  ) => Effect.Effect<boolean, ProjectionFault>;
  readonly state: (
    key: ProjectionKey,
    output: string,
    streamId: string,
  ) => Effect.Effect<"absent" | "live" | "forgotten", ProjectionFault>;
  readonly add: (
    key: ProjectionKey,
    output: string,
    streamId: string,
  ) => Effect.Effect<void, ProjectionFault>;
  /** Lists every route record for the key in unspecified order. */
  readonly list: (
    key: ProjectionKey,
  ) => Effect.Effect<ReadonlyArray<RoutedStreamRecord>, ProjectionFault>;
  /** Mark every route record for the key as forgotten. */
  readonly forget: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
  readonly clear: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
}

export interface CheckpointStoreApi {
  readonly load: (key: ProjectionKey) => Effect.Effect<Loaded, ProjectionFault>;
  /**
   * Lists each generation and encoded params with a live checkpoint for an id.
   * Reads every stored version slot, including legacy slots. Each pair appears once,
   * in unspecified order.
   */
  readonly list?: (id: string) => Effect.Effect<ReadonlyArray<ListedBinding>, ProjectionFault>;
  /** Generation-owned streams opened lazily by routed outputs. */
  readonly routes?: RouteRecords;
  /** Compare-and-set inside the owner transaction; returns the new token. */
  readonly compareAndSet: (
    key: ProjectionKey,
    ifToken: string,
    record: CheckpointRecord,
  ) => Effect.Effect<string, ProjectionFault>;
  /** Deletes a live record. Absent keys and tombstones are ignored. */
  readonly remove: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
  /** Replaces a live, absent or undecodable record with a tombstone. */
  readonly retire?: (key: ProjectionKey) => Effect.Effect<RetirementStatus, ProjectionFault>;
}

const storageFailure =
  (phase: ProjectionFault["phase"], message: string) => (cause: StorageFault) =>
    new ProjectionFault({ phase, reason: "storage-failure", message, cause });

/**
 * SQL storage fails a write with this operation when the transaction read the key as
 * absent and a concurrent save created it first. That save won the compare-and-set.
 */
const OWNER_RECORD_CONFLICT = "records.conflict";

type EncodedEntries = ReadonlyArray<readonly [key: string, encoded: string]>;

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
          list: (prefix) =>
            records.list(prefix).pipe(
              Effect.mapError(
                (cause) =>
                  new ProjectionFault({
                    phase: "load",
                    reason:
                      cause.operation === OWNER_RECORDS_LIST_UNSUPPORTED
                        ? "unsupported-composition"
                        : "storage-failure",
                    message: cause.message,
                    cause,
                  }),
              ),
            ),
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
        list: (prefix) =>
          lock.withPermits(1)(
            Effect.sync(() =>
              Array.filter(Array.fromIterable(records), ([key]) => key.startsWith(prefix)),
            ),
          ),
        read: (key) => Effect.sync(() => Option.fromUndefinedOr(records.get(key))),
        write: (key, value) =>
          Effect.sync(() => {
            records.set(key, value);
          }),
        remove: (key) =>
          Effect.sync(() => {
            records.delete(key);
          }),
        withTransaction: lock.withPermits(1),
      });
    }),
  );
}

/** The encoded seam a checkpoint Layer builds on; both shipped Layers use it. */
export interface EncodedStore {
  /** A snapshot of the stored entries whose keys start with the exact prefix. */
  readonly list?: (prefix: string) => Effect.Effect<EncodedEntries, ProjectionFault>;
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  readonly write: (key: string, value: string) => Effect.Effect<void, ProjectionFault>;
  readonly remove: (key: string) => Effect.Effect<void, ProjectionFault>;
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectionFault, R>;
}

const decodeStored = (id: string, encoded: string) =>
  Schema.decodeEffect(StoredJson)(encoded).pipe(
    Effect.mapError(
      () =>
        new ProjectionFault({
          phase: "load",
          reason: "invalid-record",
          message: `Invalid stored record for ${id}`,
        }),
    ),
  );

const ROUTED_NAMESPACE = "streamsy.projection.routed.v1";

const routeKeyParts = (key: ProjectionKey) => [
  ROUTED_NAMESPACE,
  key.id,
  STORED_VERSION_SLOT,
  String(key.generation),
  canonicalParams(key.params),
];

const RouteKeyJson = Schema.fromJsonString(
  Schema.Tuple([
    Schema.Literal(ROUTED_NAMESPACE),
    Schema.String,
    Schema.String,
    Schema.String,
    Schema.String,
    Schema.String,
    Schema.String,
  ]),
);

const RouteValueJson = Schema.fromJsonString(
  Schema.Struct({ forgotten: Schema.optionalKey(Schema.Literal(true)) }),
);

const routeSlot = (key: ProjectionKey, output: string, streamId: string) =>
  encodeKey([...routeKeyParts(key), output, streamId]);

const routeRecords = (store: EncodedStore): RouteRecords => {
  const entries = (key: ProjectionKey) =>
    store.list === undefined
      ? Effect.fail(
          new ProjectionFault({
            phase: "checkpoint",
            reason: "unsupported-composition",
            message: "This CheckpointStore does not support listing routed outputs",
          }),
        )
      : store.list(`${encodeKey(routeKeyParts(key)).slice(0, -1)},`);

  return {
    has: (key, output, streamId) =>
      store.read(routeSlot(key, output, streamId)).pipe(Effect.map(Option.isSome)),
    state: (key, output, streamId) =>
      Effect.gen(function* () {
        const encoded = yield* store.read(routeSlot(key, output, streamId));

        if (Option.isNone(encoded)) return "absent";

        const value = yield* Schema.decodeEffect(RouteValueJson, {
          onExcessProperty: "error",
        })(encoded.value).pipe(
          Effect.mapError(
            (cause) =>
              new ProjectionFault({
                phase: "process",
                reason: "invalid-record",
                message: `Invalid routed output record for ${key.id}: ${output} at ${streamId}`,
                cause,
              }),
          ),
        );

        return value.forgotten === true ? "forgotten" : "live";
      }),
    add: (key, output, streamId) => store.write(routeSlot(key, output, streamId), "{}"),
    list: (key) =>
      Effect.flatMap(entries(key), (rows) =>
        Effect.forEach(rows, ([slot]) =>
          Schema.decodeEffect(RouteKeyJson)(slot).pipe(
            Effect.map(([, , , , , output, streamId]) => ({ output, streamId })),
            Effect.mapError(
              (cause) =>
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "invalid-record",
                  message: `Invalid routed output record for ${key.id}`,
                  cause,
                }),
            ),
          ),
        ),
      ),
    forget: (key) =>
      Effect.flatMap(entries(key), (rows) =>
        Effect.forEach(rows, ([slot]) => store.write(slot, '{"forgotten":true}'), {
          discard: true,
        }),
      ),
    clear: (key) =>
      Effect.flatMap(entries(key), (rows) =>
        Effect.forEach(rows, ([slot]) => store.remove(slot), { discard: true }),
      ),
  };
};

export const fromStore = (store: EncodedStore): CheckpointStoreApi => {
  const list = Effect.fn("Projection.CheckpointStore.list")(function* (id: string) {
    if (store.list === undefined) return yield* unsupportedBindingList();

    // Replace the closing bracket with a separator after the exact namespace and id.
    const prefix = `${encodeKey([CHECKPOINT_NAMESPACE, id]).slice(0, -1)},`;
    const entries = yield* store.list(prefix);
    const byKey = new Map<string, ListedBinding>();

    for (const [key, encoded] of entries) {
      const [, , , generation, params] = yield* Schema.decodeEffect(CheckpointKeyJson)(key).pipe(
        Effect.mapError(
          (cause) =>
            new ProjectionFault({
              phase: "load",
              reason: "invalid-record",
              message: `Invalid checkpoint key for ${id}`,
              cause,
            }),
        ),
      );

      const stored = yield* decodeStored(id, encoded);

      if ("retired" in stored || stored.record.ended === true) continue;
      byKey.set(`${generation}:${canonicalParams(params)}`, { generation, params });
    }

    return Array.fromIterable(byKey.values());
  });

  const load = Effect.fn("Projection.CheckpointStore.load")(function* (
    key: ProjectionKey,
  ): Effect.fn.Return<Loaded, ProjectionFault> {
    const encoded = yield* store.read(recordKey(key));

    if (Option.isNone(encoded)) return { record: Option.none(), token: ABSENT_TOKEN };

    const envelope = yield* decodeStored(key.id, encoded.value);

    if ("retired" in envelope)
      return yield* new ProjectionFault({
        phase: "load",
        reason: "retired-generation",
        message: `Generation ${key.generation} of ${key.id} is retired. Raise generation to run it again.`,
      });

    return { record: Option.some(envelope.record), token: String(envelope.version) };
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
    list,
    routes: routeRecords(store),
    load,
    compareAndSet,
    remove: (key) =>
      store.withTransaction(
        Effect.gen(function* () {
          const slot = recordKey(key);

          const encoded = yield* store.read(slot).pipe(
            Effect.mapError(
              (fault) =>
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: fault.reason,
                  message: fault.message,
                  cause: fault.cause,
                }),
            ),
          );

          // An absent SQL row cannot be locked. Skip DELETE so it can't race a first retire.
          if (Option.isNone(encoded)) return;

          // Forget can still delete a malformed live record.
          const current = Schema.decodeOption(StoredJson)(encoded.value);

          if (Option.isSome(current) && "retired" in current.value) return;

          yield* store.remove(slot);
        }),
      ),
    retire: (key) =>
      store.withTransaction(
        Effect.gen(function* (): Effect.fn.Return<RetirementStatus, ProjectionFault> {
          const slot = recordKey(key);
          const existing = yield* store.read(slot);
          const current = Option.flatMap(existing, Schema.decodeOption(StoredJson));

          if (Option.isSome(current) && "retired" in current.value) return "already-retired";

          const version = Option.isNone(current) ? 1 : current.value.version + 1;

          const encoded = yield* Schema.encodeEffect(TombstoneJson)({ version, retired: {} }).pipe(
            Effect.mapError(
              () =>
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "invalid-record",
                  message: `Cannot encode tombstone for ${key.id}`,
                }),
            ),
          );

          yield* store.write(slot, encoded);

          return "retired";
        }),
      ),
  };
};

export const unsupportedBindingList = () =>
  new ProjectionFault({
    phase: "load",
    reason: "unsupported-composition",
    message: "This CheckpointStore does not support listing bindings",
  });

/** What the kernel needs to locate a record; every projection value satisfies it. */
export interface Identity {
  readonly id: string;
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

/** Items and terminal outcomes contribute ranges, including zero-length terminal ranges. */
export const rangesOf = (slices: Record<string, Slice<unknown>>): Record<string, Range> =>
  Record.fromEntries(
    Array.map(
      Array.filter(
        Record.toEntries(slices),
        ([, slice]) => slice.items.length > 0 || slice.closed || slice.gone === true,
      ),
      ([name, slice]): [string, Range] => [
        name,
        { from: slice.from, nextOffset: slice.nextOffset, ...terminalOf(slice) },
      ],
    ),
  );
