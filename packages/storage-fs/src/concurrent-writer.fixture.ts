/**
 * Multi-writer test fixture (NOT a test file — run as a separate `bun` process).
 *
 * Each invocation is an independent process with its own adapter/state, so the
 * only coordination is the on-disk cross-process lock. Every writer races to
 * append the SAME first message (expectedOffset = ZERO, target offset 1), so a
 * correct adapter lets exactly one win and rejects the rest with a stale-offset
 * precondition failure. Prints the resulting status to stdout.
 *
 * Usage: bun concurrent-writer.fixture.ts <root> <streamId> <value>
 */
import { createFsStorageAdapter } from "./adapter.ts";

const ZERO = `${"0".repeat(16)}_${"0".repeat(16)}`;
const OFFSET_1 = `${"1".padStart(16, "0")}_${"0".repeat(16)}`;

const [root, id, value] = process.argv.slice(2);
if (!root || !id || value === undefined) {
  process.stderr.write("usage: concurrent-writer.fixture.ts <root> <streamId> <value>\n");
  process.exit(2);
}

const adapter = createFsStorageAdapter({ root, lock: { timeoutMs: 5000, retryMs: 5 } });
const result = await adapter.append(id, {
  preconditions: { expectedOffset: ZERO, expectedClosed: false },
  messages: [{ data: new TextEncoder().encode(value), offset: OFFSET_1, timestamp: 0 }],
  recordPatch: { currentOffset: OFFSET_1, counter: 1 },
});

process.stdout.write(result.status);
