import { Layer } from "effect";
import * as BunStorage from "@streamsy/storage/bun";
import * as ProjectionSql from "@streamsy/projection/sql";

export const layerSqlite = (readLimit = 1000) =>
  ProjectionSql.layerStorage.pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" }, readLimit })),
  );
