import { CheckpointStore, Transaction } from "@streamsy/projection";
import { Layer } from "effect";
import * as BunStorage from "@streamsy/storage/bun";

export const layerSqlite = (readLimit = 1000) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" }, readLimit })),
  );
