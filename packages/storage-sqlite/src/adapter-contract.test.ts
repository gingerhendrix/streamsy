/**
 * Storage-adapter contract against the Bun SQLite adapter.
 *
 * Runs the shared `runStorageAdapterContract` kit over a fresh isolated
 * `:memory:` database per case under `bun:test` (the SQLite adapter needs the Bun
 * runtime for `bun:sqlite`). Proves the SQLite engine satisfies the same flat
 * `StorageAdapter` seam contract as every other adapter.
 */
import { describe, it } from "bun:test";
import { runStorageAdapterContract } from "@streamsy/core";
import { createSqliteStorageAdapter } from "./adapter.ts";

describe("sqlite adapter — storage contract", () => {
  runStorageAdapterContract(() => createSqliteStorageAdapter(), { it });
});
