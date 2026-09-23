import { Context, Effect, Option, Schema } from "effect";
import { ZERO_OFFSET } from "@streamsy/core";
import type { InputMap, Slice } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import { Counter, PendingUnit, canonicalParams, encodeKey, type Range } from "./unit.ts";

export const CheckpointRecord = Schema.Struct({
  identity: Schema.Struct({ inputs: Schema.Record(Schema.String, Schema.String) }),
  /** Input key to accepted offset; the next pass reads after these. */
  inputs: Schema.Record(Schema.String, Schema.String),
  pending: Schema.optionalKey(PendingUnit),
  adapters: Schema.Struct({
    outputs: Schema.optionalKey(
      Schema.Record(Schema.String, Schema.Struct({ epoch: Counter, nextSeq: Counter })),
    ),
  }),
});
export interface CheckpointRecord extends Schema.Schema.Type<typeof CheckpointRecord> {}

/** Stored value. The version is the compare-and-set token exposed as a string. */
export const Envelope = Schema.Struct({ version: Counter, record: CheckpointRecord });
export interface Envelope extends Schema.Schema.Type<typeof Envelope> {}
export const EnvelopeJson = Schema.fromJsonString(Envelope);

export interface ProjectionKey {
  readonly id: string;
  readonly version?: number;
  readonly generation: number;
  readonly params: Record<string, string>;
}
export const recordKey = (key: ProjectionKey): string =>
  encodeKey([
    "streamsy.projection.v1",
    key.id,
    ...(key.version === undefined || key.version === 1 ? [] : [String(key.version)]),
    String(key.generation),
    canonicalParams(key.params),
  ]);

/** Token "0" means no record is stored yet. */
export const ABSENT_TOKEN = "0";
export interface Loaded {
  readonly record: Option.Option<CheckpointRecord>;
  readonly token: string;
}
export interface CheckpointsApi {
  readonly load: (key: ProjectionKey) => Effect.Effect<Loaded, ProjectionFault>;
  /** Compare-and-set inside the owner transaction; returns the new token. */
  readonly save: (
    key: ProjectionKey,
    record: CheckpointRecord,
    ifToken: string,
  ) => Effect.Effect<string, ProjectionFault>;
  /** Deletes the record inside the caller's transaction. */
  readonly remove: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectionFault, R>;
}
/** One host owner supplies the record store and the transaction the fused handler joins. */
export class Checkpoints extends Context.Service<Checkpoints, CheckpointsApi>()(
  "@streamsy/projection/Checkpoints",
) {}

/** The encoded seam a host owner provides; both shipped Layers build on it. */
export interface EncodedStore {
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  readonly write: (key: string, value: string) => Effect.Effect<void, ProjectionFault>;
  readonly remove: (key: string) => Effect.Effect<void, ProjectionFault>;
  readonly readState: (key: string) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  readonly writeState: (key: string, value: string) => Effect.Effect<void, ProjectionFault>;
  readonly removeState: (key: string) => Effect.Effect<void, ProjectionFault>;
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectionFault, R>;
}
export const fromStore = (store: EncodedStore): CheckpointsApi => {
  const load = Effect.fn("Projection.Checkpoints.load")(function* (key: ProjectionKey) {
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
  const save = Effect.fn("Projection.Checkpoints.save")(function* (
    key: ProjectionKey,
    record: CheckpointRecord,
    ifToken: string,
  ) {
    return yield* store.withTransaction(
      Effect.gen(function* () {
        const current = yield* load(key);
        if (current.token !== ifToken)
          return yield* new ProjectionFault({
            phase: "checkpoint",
            reason: "token-conflict",
            message: `Record for ${key.id} changed: expected token ${ifToken}, found ${current.token}`,
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
    save,
    remove: (key) => store.remove(recordKey(key)),
    withTransaction: store.withTransaction,
  };
};

/** What the kernel needs to locate a record; every projection value satisfies it. */
export interface Identity {
  readonly id: string;
  readonly version?: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: InputMap;
}
const identityOf = (inputs: InputMap): Record<string, string> =>
  Object.fromEntries(
    Object.entries(inputs).map(([name, ref]) => [name, encodeKey([ref.id, ref.contentType])]),
  );
const sameIdentity = (a: Record<string, string>, b: Record<string, string>): boolean => {
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((k) => a[k] === b[k]);
};

/** The stored record, or the unstored initial record for a fresh key. */
export const restore = Effect.fn("Projection.restore")(function* (
  projection: Identity,
  owner: CheckpointsApi,
) {
  const loaded = yield* owner.load(projection);
  const identity = { inputs: identityOf(projection.inputs) };
  if (Option.isNone(loaded.record)) {
    const record: CheckpointRecord = {
      identity,
      inputs: Object.fromEntries(Object.keys(projection.inputs).map((name) => [name, ZERO_OFFSET])),
      adapters: {},
    };
    return { record, token: loaded.token };
  }
  if (!sameIdentity(loaded.record.value.identity.inputs, identity.inputs))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "identity-mismatch",
      message: `Stored inputs differ from the declaration of ${projection.id}`,
    });
  for (const name of Object.keys(projection.inputs)) {
    if (loaded.record.value.inputs[name] === undefined)
      return yield* new ProjectionFault({
        phase: "load",
        reason: "invalid-record",
        input: name,
        message: `Stored record of ${projection.id} has no offset for ${name}`,
      });
  }
  return { record: loaded.record.value, token: loaded.token };
});

/** Accepted offsets after a unit: every read input moves to its `nextOffset`. */
export const advance = (
  inputs: Record<string, string>,
  ranges: Record<string, { readonly nextOffset: string }>,
): Record<string, string> =>
  Object.fromEntries(
    Object.entries(inputs).map(([name, offset]) => [name, ranges[name]?.nextOffset ?? offset]),
  );
/** Only the inputs that contributed items; the unit key and any pin come from these. */
export const rangesOf = (slices: Record<string, Slice<unknown>>): Record<string, Range> =>
  Object.fromEntries(
    Object.entries(slices)
      .filter(([, slice]) => slice.items.length > 0)
      .map(([name, slice]) => [name, { from: slice.from, nextOffset: slice.nextOffset }]),
  );

export { PendingUnit, PinnedRange, encodeKey } from "./unit.ts";

export { stateFromStore } from "./state.ts";
