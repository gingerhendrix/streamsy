/**
 * Runnable, deterministic signature Risk demo proof.
 *
 * Runs the whole signature sequence over real Streamsy storage + the HTTP
 * command/turn resources, streams structured JSONL trace events for the article,
 * verifies the demo invariants, prints a machine-readable summary, and exits 0
 * on success (non-zero on any invariant failure).
 *
 *   bun run scripts/proof.ts                 # in-memory Streamsy storage
 *   DB_PATH=./proof.sqlite bun run scripts/proof.ts   # real SQLite durability
 *   TRACE_FILE=./trace.jsonl bun run scripts/proof.ts # also write trace to a file
 *
 * Trace output is never committed: it prints to stdout (or a path you choose).
 */

import { appendFileSync, rmSync } from "node:fs";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";

import { createInMemoryStores } from "../server/persistence/stores.ts";
import {
  runSignatureDemo,
  type DemoSummary,
  type TraceEvent,
} from "../server/demo/signature-demo.ts";

const seed = Number.parseInt(process.env.SEED ?? "1234", 10);
const dbPath = process.env.DB_PATH;
const traceFile = process.env.TRACE_FILE;

async function makeStorage(): Promise<{
  protocol: ReturnType<typeof createStreamProtocol>;
  stores: ReturnType<typeof createInMemoryStores>;
  mode: string;
  close: () => void;
}> {
  if (dbPath) {
    // SQLite mode (durable). Imported lazily so the default memory run has no
    // bun:sqlite dependency.
    const { createSqliteStorageAdapter } = await import("@streamsy/storage-sqlite");
    const { createSqliteStores } = await import("../server/persistence/sqlite-store.ts");
    const adapter = createSqliteStorageAdapter({ filename: dbPath });
    const protocol = createStreamProtocol({ storage: { adapter } });
    const stores = createSqliteStores(adapter.state.db);
    return { protocol, stores, mode: `sqlite:${dbPath}`, close: () => adapter.close() };
  }
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  return { protocol, stores: createInMemoryStores(), mode: "memory", close: () => {} };
}

function checkInvariants(summary: DemoSummary): string[] {
  const failures: string[] = [];
  const require = (ok: boolean, message: string): void => {
    if (!ok) failures.push(message);
  };
  require(summary.winnerId !== null, "game did not finish with a winner");
  require(summary.finalWatermark !== null, "final board watermark missing");
  require(summary.crashRecovery
    .boardEqual, "post-crash board did not equal the authoritative fold");
  require(!summary.crashRecovery.doubleApplied, "crash-after-output double-applied a transition");
  require(summary.crashRecovery.duplicateSourceSeqs.length ===
    0, `a source ordinal was applied twice: ${summary.crashRecovery.duplicateSourceSeqs.join(", ")}`);
  require(summary.crashRecovery.actualTransitions ===
    summary.crashRecovery
      .canonicalEvents, "recovered projection does not have exactly one transition per canonical event");
  require(summary.crashRecovery.actualTransitions === summary.crashRecovery.expectedTransitions &&
    summary.crashRecovery.actualOutputMessages ===
      summary.crashRecovery
        .expectedOutputMessages, "recovered projection differs from a clean control build of the same log");
  require(summary.crashRecovery
    .watermarkEqual, "recovered watermark differs from the control build");
  require(summary.rebuild.boardEqual &&
    summary.rebuild.watermarkEqual, "rebuild verification failed");
  require(summary.rebuild.activeGeneration ===
    summary.rebuild.toGeneration, "rebuild did not cut over the active generation");
  require(summary.rebuild.retained.length >= 2, "old generation was not retained after cutover");
  require(summary.idempotentRetry?.duplicate ===
    true, "idempotent retry was not classified as duplicate");
  require(summary.causalWait?.synced ===
    true, "causal syncedThrough did not confirm the acked offset");
  require(summary.staleCommand?.rejected === true, "stale/racing command was not rejected");
  require(summary.attackRecorded !== null, "no accepted attack with recorded dice was captured");
  return failures;
}

async function main(): Promise<void> {
  if (traceFile) rmSync(traceFile, { force: true });
  const storage = await makeStorage();
  const emit = (event: TraceEvent): void => {
    const line = JSON.stringify(event);
    process.stdout.write(`${line}\n`);
    if (traceFile) appendFileSync(traceFile, `${line}\n`);
  };

  try {
    const { summary } = await runSignatureDemo({
      protocol: storage.protocol,
      stores: storage.stores,
      seed,
      emit,
    });
    const failures = checkInvariants(summary);
    process.stdout.write(`\nSUMMARY ${JSON.stringify(summary)}\n`);
    if (failures.length > 0) {
      console.error(`✗ signature demo FAILED (${storage.mode}):`);
      for (const f of failures) console.error(`  - ${f}`);
      process.exit(1);
    }
    console.log(
      `✓ signature demo passed (${storage.mode}): winner=${summary.winnerId}, ` +
        `watermark=${summary.finalWatermark}, rebuild ${summary.rebuild.fromGeneration}→${summary.rebuild.toGeneration}, ` +
        `crash-double-applied=${summary.crashRecovery.doubleApplied}`,
    );
  } finally {
    storage.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
