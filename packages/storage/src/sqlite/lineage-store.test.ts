import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { ZERO_OFFSET, type StreamRecord } from "@streamsy/core";
import { createSqliteStorageAdapter } from "./adapter.ts";
import { SqliteLineageStore } from "./lineage-store.ts";

test("a renewal on another connection defeats a stale lineage soft delete", async () => {
  const root = mkdtempSync(join(tmpdir(), "streamsy-sqlite-lineage-"));
  const filename = join(root, "streamsy.sqlite");
  const adapter = createSqliteStorageAdapter({ filename });
  const renewal = new Database(filename);
  const originalDeadline = 10_000;
  const renewedDeadline = 20_000;
  const record: StreamRecord = {
    id: "renewed",
    config: { contentType: "text/plain", ttlSeconds: 10, createdAt: 0 },
    lifecycle: { expiresAtMs: originalDeadline },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };

  try {
    expect((await adapter.create({ record })).status).toBe("created");
    const lineage = new SqliteLineageStore(adapter.state.db, adapter.state);
    expect((await lineage.getRecord("renewed"))?.lifecycle.expiresAtMs).toBe(originalDeadline);

    renewal.run("update streamsy_streams set expires_at_ms = ? where stream_id = ?", [
      renewedDeadline,
      "renewed",
    ]);

    expect(await lineage.softDelete("renewed", originalDeadline)).toBe(false);
    expect(await lineage.getRecord("renewed")).toMatchObject({
      lifecycle: { expiresAtMs: renewedDeadline, softDeleted: false },
    });
  } finally {
    renewal.close();
    adapter.close();
    rmSync(root, { recursive: true, force: true });
  }
});
