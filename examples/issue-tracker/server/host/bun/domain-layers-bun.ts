/** Bun-only placement layers, kept out of the Cloudflare Worker module graph. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Context, Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import { migratedGlobalSqlLayer, type GlobalServices } from "../../exchange/global-domain.ts";
import { migratedInboxSqlLayer, type InboxStore } from "../../exchange/inbox-store.ts";

const clientLayer = (filename: string): Layer.Layer<SqlClient.SqlClient> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* SqliteClient.make({ filename, create: true });
      yield* client.unsafe<Record<string, never>>("PRAGMA journal_mode = WAL").pipe(Effect.asVoid);
      return Context.empty().pipe(Context.add(SqlClient.SqlClient, client));
    }),
  ).pipe(Layer.provide(Reactivity.layer), Layer.orDie);

export const userSqliteLayer = (filename: string): Layer.Layer<InboxStore> =>
  migratedInboxSqlLayer.pipe(Layer.orDie, Layer.provide(clientLayer(filename)));

export const globalSqliteLayer = (filename: string): Layer.Layer<GlobalServices> =>
  migratedGlobalSqlLayer.pipe(Layer.orDie, Layer.provide(clientLayer(filename)));
