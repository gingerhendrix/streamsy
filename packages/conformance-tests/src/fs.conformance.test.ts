/**
 * Run conformance tests against the JSONL filesystem server implementation.
 *
 * Starts a server with `@streamsy/storage-fs` pointed at a fresh temp directory,
 * runs the official @durable-streams/server-conformance-tests suite over HTTP,
 * then stops the server and removes the temp tree.
 *
 * The fs adapter is pure `node:fs`/`node:path`, so (unlike sqlite) it runs
 * in-process under the vitest/node runtime — same bootstrap as the memory entry.
 *
 * `watch` defaults OFF (`FS_WATCH=1` to enable). `awaitChange` is required on
 * the seam, so both modes drive the adapter's own `runAwaitChangeLoop`-based
 * implementation. Off ⇒ long-poll / SSE run on the in-process notifier plus the
 * capped park (the polling floor). On ⇒ `fs.watch` is raced in as the
 * cross-process wake source, exercised end-to-end through the HTTP/SSE stack
 * (the HTTP-frontend use-case).
 *
 * **Fork is excluded by design.** The v1 fs adapter is intentionally forkless
 * (see packages/storage-fs), so the protocol surfaces forks as `not-supported`
 * (HTTP 400). The official suite has no capability-skip option, so the
 * `test:fs` script filters out the `Fork - *` groups with a vitest
 * `--testNamePattern` negative lookahead (`-t '^(?!.*Fork - )'`). Every other
 * capability runs green. Drop the filter once fork lands.
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { describe, beforeAll, afterAll } from "vitest";
import { StreamProtocol, HttpHandler } from "@streamsy/core";
import { createFsStorageAdapter } from "@streamsy/storage-fs";

let server: { stop: () => void; port: number | undefined } | null = null;
let tempDir: string | null = null;
const watch = process.env.FS_WATCH === "1";

describe(`Filesystem Storage Server Implementation${watch ? " (watch)" : ""}`, () => {
  const port = 19537 + Math.floor(Math.random() * 1000);
  const config = {
    baseUrl: `http://localhost:${port}`,
  };

  beforeAll(async () => {
    tempDir = mkdtempSync(join(tmpdir(), "streamsy-fs-conf-"));
    const adapter = createFsStorageAdapter({ root: tempDir, watch, watchPollMs: 250 });
    const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1500 });
    const handler = new HttpHandler({ protocol, pathPrefix: "/" });

    // Use globalThis.Bun for Bun runtime, fall back to node:http.
    if (typeof Bun !== "undefined") {
      server = Bun.serve({
        port,
        fetch: (req: Request) => handler.fetch(req),
      });
    } else {
      // Node.js fallback
      const { createServer } = await import("node:http");
      const nodeServer = createServer(async (req, res) => {
        const url = `http://localhost:${port}${req.url}`;
        const headers = new Headers();
        for (const [key, value] of Object.entries(req.headers)) {
          if (value) headers.set(key, Array.isArray(value) ? value[0]! : value);
        }

        const body = await new Promise<Buffer>((resolve) => {
          const chunks: Buffer[] = [];
          req.on("data", (chunk: Buffer) => chunks.push(chunk));
          req.on("end", () => resolve(Buffer.concat(chunks)));
        });

        const request = new Request(url, {
          method: req.method,
          headers,
          body: ["GET", "HEAD"].includes(req.method!) ? undefined : body,
          duplex: "half",
        } as RequestInit);

        const response = await handler.fetch(request);

        const responseHeaders: Record<string, string> = {};
        response.headers.forEach((value, key) => {
          responseHeaders[key] = value;
        });
        res.writeHead(response.status, responseHeaders);
        if (response.body) {
          const reader = response.body.getReader();
          const pump = async () => {
            while (true) {
              const { done, value } = await reader.read();
              if (done) {
                res.end();
                break;
              }
              res.write(value);
            }
          };
          await pump();
        } else {
          res.end();
        }
      });

      await new Promise<void>((resolve) => {
        nodeServer.listen(port, () => resolve());
      });

      server = {
        port,
        stop: () => nodeServer.close(),
      };
    }
  });

  afterAll(() => {
    server?.stop();
    if (tempDir) rmSync(tempDir, { recursive: true, force: true });
  });

  runConformanceTests(config);
});
