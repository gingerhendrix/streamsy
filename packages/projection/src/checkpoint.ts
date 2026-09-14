import { Context, Effect, Option, Schema } from "effect";
import { ProjectionFault } from "./fault.ts";
import { Counter, PendingUnit, canonicalParams, encodeKey } from "./unit.ts";

export const CheckpointRecord = Schema.Struct({
  identity: Schema.Struct({ inputs: Schema.Record(Schema.String, Schema.String) }),
  /** Input key to accepted offset; the next pass reads after these. */
  inputs: Schema.Record(Schema.String, Schema.String),
  pending: Schema.optionalKey(PendingUnit),
  adapters: Schema.Struct({
    stream: Schema.optionalKey(Schema.Struct({ epoch: Counter, nextSeq: Counter })),
  }),
});
export interface CheckpointRecord extends Schema.Schema.Type<typeof CheckpointRecord> {}

/** Stored value. The version is the compare-and-set token exposed as a string. */
export const Envelope = Schema.Struct({ version: Counter, record: CheckpointRecord });
export interface Envelope extends Schema.Schema.Type<typeof Envelope> {}
export const EnvelopeJson = Schema.fromJsonString(Envelope);

export interface ProjectionKey {
  readonly id: string;
  readonly generation: number;
  readonly params: Record<string, string>;
}
export const recordKey = (key: ProjectionKey): string =>
  encodeKey([
    "streamsy.projection.v1",
    key.id,
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
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectionFault, R>;
}
/** One host owner supplies the record store and the transaction the fused handler joins. */
export class Checkpoints extends Context.Service<Checkpoints, CheckpointsApi>()(
  "@streamsy/projection/Checkpoints",
) {}

/** The encoded seam a host owner provides; both first-cut Layers build on it. */
export interface EncodedStore {
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  readonly write: (key: string, value: string) => Effect.Effect<void, ProjectionFault>;
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
  return { load, save, withTransaction: store.withTransaction };
};
