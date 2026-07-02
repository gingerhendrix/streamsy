/**
 * Run conformance tests against the in-memory server implementation.
 *
 * Starts a Bun server with memory storage, runs the official
 * @durable-streams/server-conformance-tests suite, then cleans up.
 */

import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { describe, beforeAll, afterAll } from "vitest";
import { StreamProtocol, HttpHandler, createMemoryStorageAdapter } from "@streamsy/core";

let server: { stop: () => void; port: number | undefined } | null = null;

describe("Memory Storage Server Implementation", () => {
  const port = 19337 + Math.floor(Math.random() * 1000);
  const config = {
    baseUrl: `http://localhost:${port}`,
  };

  beforeAll(async () => {
    const adapter = createMemoryStorageAdapter();
    const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1500 });
    const handler = new HttpHandler({ protocol, pathPrefix: "/" });

    // Use globalThis.Bun for Bun runtime, fall back to node:http
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
  });

  runConformanceTests(config);
});
