/**
 * Run conformance tests against the Bun SQLite server implementation.
 *
 * The official suite runs under vitest (node), but the SQLite adapter needs the
 * Bun runtime for `bun:sqlite`. So we spawn `sqlite-server.ts` with the `bun`
 * binary, wait for it to report its port, run the suite over HTTP, then tear the
 * process and its temp database down.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { describe, beforeAll, afterAll } from "vitest";

let child: ChildProcess | null = null;
let tempDir: string | null = null;
const port = 19437 + Math.floor(Math.random() * 1000);
const config = { baseUrl: `http://localhost:${port}` };

function waitForListening(proc: ChildProcess): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("sqlite-server did not start in time")), 15000);
    proc.stdout?.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("LISTENING")) {
        clearTimeout(timer);
        resolve();
      }
    });
    proc.stderr?.on("data", (chunk: Buffer) => {
      process.stderr.write(`[sqlite-server] ${chunk.toString()}`);
    });
    proc.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`sqlite-server exited early with code ${code}`));
    });
  });
}

describe("SQLite Storage Server Implementation", () => {
  beforeAll(async () => {
    tempDir = mkdtempSync(join(tmpdir(), "streamsy-sqlite-conf-"));
    const dbPath = join(tempDir, "conformance.sqlite");
    const serverPath = join(import.meta.dirname, "sqlite-server.ts");
    child = spawn("bun", [serverPath], {
      env: { ...process.env, PORT: String(port), DB_PATH: dbPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    await waitForListening(child);
  });

  afterAll(() => {
    child?.kill("SIGTERM");
    if (tempDir) rmSync(tempDir, { recursive: true, force: true });
  });

  runConformanceTests(config);
});
