// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- Real child-process host over the Web fetch edge.
/**
 * The official suite and the live-read checks for one runtime listener. Each
 * describe spawns its own host with SQLite through `node:sqlite` and skips
 * when the runtime binary is missing.
 */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { runtimeCommand, spawnHost, type Runtime, type SpawnedHost } from "./spawned-host.ts";

const HOLD_MS = 4_000;

export const describeRuntimeHost = (runtime: Runtime) => {
  const command = runtimeCommand(runtime);

  describe.skipIf(command === undefined)(`Effect SQLite ${runtime} host`, () => {
    const config = { baseUrl: "" };
    let host: SpawnedHost | undefined;
    beforeAll(async () => {
      if (command === undefined) return;
      host = await spawnHost(command, { LONG_POLL_TIMEOUT_MS: "1500" });
      config.baseUrl = host.url;
    }, 120_000);
    afterAll(async () => {
      await host?.stop();
    });
    runConformanceTests(config);
  });

  describe.skipIf(command === undefined)(`${runtime} listener live reads`, () => {
    let host: SpawnedHost | undefined;
    const url = (path: string) => {
      if (host === undefined) throw new Error("Expected a running host");
      return new URL(path, host.url);
    };
    const tail = async (path: string) => {
      const created = await fetch(url(path), {
        method: "PUT",
        headers: { "content-type": "text/plain" },
        body: "seed",
      });
      expect(created.status).toBe(201);
      const offset = created.headers.get("stream-next-offset");
      if (offset === null) throw new Error("Expected current tail");
      return offset;
    };
    beforeAll(async () => {
      if (command === undefined) return;
      host = await spawnHost(command, {
        LONG_POLL_TIMEOUT_MS: String(HOLD_MS),
        SSE_DEADLINE_MS: String(HOLD_MS),
        // Node only: a request bound far below the hold. It limits the time to
        // receive a request, so it must not cut the parked response.
        NODE_REQUEST_TIMEOUT_MS: "1000",
      });
    }, 120_000);
    afterAll(async () => {
      await host?.stop();
    });

    it("holds a long poll until the protocol timeout and answers 204", async () => {
      const offset = await tail("long-poll");
      const startedAt = performance.now();
      const response = await fetch(url(`long-poll?offset=${offset}&live=long-poll`));
      const elapsed = performance.now() - startedAt;
      expect(response.status).toBe(204);
      expect(elapsed).toBeGreaterThanOrEqual(HOLD_MS - 250);
    }, 20_000);

    it("holds an SSE body until its deadline and ends it cleanly", async () => {
      const offset = await tail("sse");
      const startedAt = performance.now();
      const response = await fetch(url(`sse?offset=${offset}&live=sse`));
      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toContain("text/event-stream");
      const text = await response.text();
      const elapsed = performance.now() - startedAt;
      expect(text).toContain("event: control");
      // The deadline carries up to 10 percent jitter either way.
      expect(elapsed).toBeGreaterThanOrEqual(HOLD_MS * 0.9 - 250);
    }, 20_000);

    it("stops on SIGTERM with exit code 0", async () => {
      if (host === undefined) throw new Error("Expected a running host");
      const stopping = host;
      host = undefined;
      expect(await stopping.stop()).toBe(0);
    }, 20_000);
  });
};
