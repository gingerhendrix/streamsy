import { expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { ViewBackend } from "../../src/index.ts";
import { routedGroup, routedProof } from "./support/routed-proof.ts";

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer, ViewBackend.layerSql).pipe(
  Layer.provideMerge(BunStorage.layer(routedGroup, { client: { filename: ":memory:" } })),
);

it.live("routes the full-recompute oracle by workspace and date", () =>
  routedProof(false).pipe(
    Effect.scoped,
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test entry point provisions its own SQLite host.
    Effect.provide(host),
    Effect.tap((result) =>
      Effect.sync(() => expect(result).toEqual({ routes: 3, moved: true, faults: 2 })),
    ),
  ),
);
