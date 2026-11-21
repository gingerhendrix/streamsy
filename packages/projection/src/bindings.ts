import { Effect, Schema } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import { CheckpointStore, unsupportedBindingList } from "./checkpoint-store.ts";
import type { Declaration } from "./declaration.ts";
import { InvalidBinding, type ProjectionFault } from "./fault.ts";
import { canonicalParams } from "./unit.ts";

/**
 * Lists the decoded params of bindings with live checkpoints in the selected store.
 * By default it includes every generation of the declaration's id, across checkpoint
 * versions. A generation filter still includes every version at that generation.
 * Each binding appears once, in unspecified order.
 *
 * Fails with `InvalidBinding` if live params cannot round-trip through the current
 * codecs. MySQL owner stores fail because historical hashed keys cannot be decoded.
 */
export const bindings = Effect.fn("Projection.bindings")(function* <
  C extends StreamRoute.ParamCodecs,
>(
  declaration: Pick<Declaration<C, unknown>, "id" | "params">,
  options?: { readonly generation?: number },
): Effect.fn.Return<
  ReadonlyArray<StreamRoute.Params<C>>,
  InvalidBinding | ProjectionFault,
  CheckpointStore
> {
  const store = yield* CheckpointStore;

  if (store.list === undefined) return yield* unsupportedBindingList();

  const schema = Schema.Struct(declaration.params);
  const decode = Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" });
  const encode = Schema.encodeEffect(schema);
  const stored = yield* store.list(declaration.id, options?.generation);

  return yield* Effect.forEach(stored, (params) =>
    Effect.gen(function* () {
      const invalid = () => new InvalidBinding({ projection: declaration.id, params });
      const decoded = yield* decode(params).pipe(Effect.mapError(invalid));
      const encoded = yield* encode(decoded).pipe(Effect.mapError(invalid));

      if (canonicalParams(encoded) !== canonicalParams(params)) return yield* invalid();

      // SAFETY: the decoded fields use the declared codecs, and the round trip
      // proves they account for exactly the params that were stored.
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- TS cannot reduce the generic Struct optionality to Params<C>.
      return decoded as StreamRoute.Params<C>;
    }),
  );
});
