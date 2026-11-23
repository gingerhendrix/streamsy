import { Layer } from "effect";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, OwnerTransaction, Transaction } from "@streamsy/projection";
import { ViewBackend } from "../../../src/index.ts";
import { group } from "./shop.ts";

export const host = Layer.mergeAll(
  CheckpointStore.layer,
  OwnerTransaction.layer,
  ViewBackend.layerSql,
  Transaction.layer,
).pipe(
  Layer.provideMerge(BunStorage.layer(group, { client: { filename: ":memory:" }, readLimit: 200 })),
);
