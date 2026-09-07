import { SqliteClient } from "@effect/sql-sqlite-do";
import { Layer } from "effect";
import { sharedSqlClientLayer } from "./boundary.ts";
import { layer as sqlLayer, type SqlStorageOptions } from "./storage.ts";

export interface DurableObjectStorageOptions extends SqlStorageOptions {
  /** Full DurableObjectStorage is required because Storage mutations are transactional. */
  readonly client: Parameters<typeof SqliteClient.make>[0] & {
    readonly storage: NonNullable<Parameters<typeof SqliteClient.make>[0]["storage"]>;
  };
}

/** Official local/host Durable Object SQLite layer with commit-boundary support. */
export const layer = (options: DurableObjectStorageOptions) => {
  const clientLayer = Layer.effectContext(sharedSqlClientLayer(SqliteClient.make(options.client)));
  return sqlLayer(options).pipe(Layer.provide(clientLayer));
};
