import { encodeKey } from "./identity.ts";
import { Effect, Option, Schema } from "effect";
import { Identity } from "./identity.ts";
import { DeriveFault } from "./fault.ts";

export const Checkpoint = Schema.Struct({
  identity: Identity,
  sourcePosition: Schema.String,
  sinkPosition: Schema.String,
  revision: Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
});
export interface Checkpoint extends Schema.Schema.Type<typeof Checkpoint> {}
export const StateRecord = Schema.Struct({
  revision: Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  encoded: Schema.String,
});
export interface StateRecord extends Schema.Schema.Type<typeof StateRecord> {}
export interface CheckpointStore {
  readonly load: (id: string) => Effect.Effect<Option.Option<Checkpoint>, DeriveFault>;
  readonly save: (id: string, checkpoint: Checkpoint) => Effect.Effect<void, DeriveFault>;
}
export interface StateStore {
  readonly load: (id: string) => Effect.Effect<Option.Option<StateRecord>, DeriveFault>;
  readonly save: (id: string, state: StateRecord) => Effect.Effect<void, DeriveFault>;
}
export interface EncodedStore {
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, DeriveFault>;
  readonly write: (key: string, value: string) => Effect.Effect<void, DeriveFault>;
}
export const records = <A>(
  schema: Schema.Codec<A, string>,
  prefix: string,
  store: EncodedStore,
) => ({
  load: Effect.fn("Derive.Store.load")(function* (id: string) {
    const encoded = yield* store.read(encodeKey(["streamsy.derive.v1", prefix, id]));
    if (Option.isNone(encoded)) return Option.none<A>();
    return Option.some(
      yield* Schema.decodeEffect(schema)(encoded.value).pipe(
        Effect.mapError(
          () =>
            new DeriveFault({ reason: "invalid-state", message: `Invalid ${prefix} for ${id}` }),
        ),
      ),
    );
  }),
  save: Effect.fn("Derive.Store.save")(function* (id: string, value: A) {
    const encoded = yield* Schema.encodeEffect(schema)(value).pipe(
      Effect.mapError(
        () =>
          new DeriveFault({
            reason: "invalid-state",
            message: `Cannot encode ${prefix} for ${id}`,
          }),
      ),
    );
    yield* store.write(encodeKey(["streamsy.derive.v1", prefix, id]), encoded);
  }),
});
