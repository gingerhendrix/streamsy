/**
 * Host/admin CLI: rebuild a game's board projection into a fresh generation and
 * cut the durable active pointer over to it after verification.
 *
 * Not a product-UI action — it opens the SQLite database directly (the same file
 * the server uses) so it is host-only. The old generation is retained and stays
 * usable; on verification failure nothing is cut over and the exit code is
 * non-zero.
 *
 *   DB_PATH=./risk.sqlite bun run scripts/rebuild.ts <gameId> [targetGeneration]
 */

import { createStreamProtocol } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

import { createSqliteStores } from "../server/persistence/sqlite-store.ts";
import { rebuildBoardGeneration } from "../server/game/rebuild.ts";
import { createBoardRuntimeCache } from "../server/game/board.ts";

const gameId = process.argv[2] ?? process.env.GAME_ID ?? "";
const generation = process.argv[3] ?? process.env.TARGET_GENERATION ?? undefined;
const dbPath = process.env.DB_PATH ?? ":memory:";

if (!gameId) {
  console.error(
    "usage: DB_PATH=./risk.sqlite bun run scripts/rebuild.ts <gameId> [targetGeneration]",
  );
  process.exit(2);
}

const adapter = createSqliteStorageAdapter({ filename: dbPath });
const protocol = createStreamProtocol({ storage: { adapter } });
const stores = createSqliteStores(adapter.state.db);
const boardRuntime = createBoardRuntimeCache(protocol);

try {
  const result = await rebuildBoardGeneration({ protocol, stores, boardRuntime }, gameId, {
    generation,
  });
  console.log(JSON.stringify(result, null, 2));
  if (result.status === "cutover") {
    console.log(
      `✓ cut over ${gameId}: ${result.fromGeneration} → ${result.toGeneration} ` +
        `(watermark ${result.sourceThroughOffset ?? "∅"}; retained ${result.retainedGenerations.join(", ")})`,
    );
    // Worth calling out separately: a rebuild across reducer versions is the
    // migration, and until it runs the active stream still holds rows an older
    // reducer wrote.
    if (result.fromReducerVersion !== result.toReducerVersion) {
      console.log(
        `  reducer ${result.fromReducerVersion ?? "unrecorded"} → ${result.toReducerVersion}`,
      );
    }
    process.exit(0);
  }
  console.error(`✗ rebuild ${result.status} for ${gameId}; active generation unchanged.`);
  process.exit(1);
} finally {
  await boardRuntime.runtime.dispose();
  await boardRuntime.client.close();
  adapter.close();
}
