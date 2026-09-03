/* oxlint-disable effecttsgo/async-function, effecttsgo/global-date, effecttsgo/global-timers, effecttsgo/new-promise -- Bun owns this timer-bound adapter regression. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test } from "bun:test";
import { ZERO_OFFSET, type StreamRecord } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";

test("a stale expiry delete preserves the replacement generation scheduler", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "streamsy-fs-expiry-"));
  let resolveFired!: () => void;
  const fired = new Promise<void>((resolve) => {
    resolveFired = resolve;
  });
  const replacementDeadline = Date.now() + 200;
  const record: StreamRecord = {
    id: "renewed",
    config: { contentType: "text/plain", ttlSeconds: 60, createdAt: Date.now() },
    lifecycle: { expiresAtMs: replacementDeadline },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
  const adapter = createFsStorageAdapter({ root, onScheduledExpiry: resolveFired });

  try {
    expect((await adapter.create({ record })).status).toBe("created");
    await adapter.scheduleExpiry("renewed", replacementDeadline);
    const handle = adapter.state.getExistingStream("renewed");

    expect(
      await adapter.delete({
        streamId: "renewed",
        reason: "expiry",
        expectedExpiresAtMs: replacementDeadline - 1,
      }),
    ).toEqual({ status: "expiry-mismatch" });
    expect(adapter.state.getExistingStream("renewed")).toBe(handle);
    expect((await adapter.getRecord("renewed"))?.lifecycle.expiresAtMs).toBe(replacementDeadline);

    await Promise.race([
      fired,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("replacement expiry callback did not fire")), 2_000),
      ),
    ]);
  } finally {
    await adapter.cancelExpiry("renewed");
    rmSync(root, { recursive: true, force: true });
  }
});
