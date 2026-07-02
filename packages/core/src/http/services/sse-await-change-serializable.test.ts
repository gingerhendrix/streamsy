/**
 * Regression guard for the original Cloudflare Durable Object failure:
 * `DataCloneError: AbortSignal could not be cloned`.
 *
 * An SSE live read drives the full stack — `SseHttpService` → `ProtocolStream.readLive`
 * → `LiveReadService` → `raceAbortAwaitChange` → `StorageAdapter.awaitChange`. The
 * caller-local `AbortSignal` must stay at the HTTP/SSE edge and NEVER reach the
 * storage seam. The primary assertion is structural — every argument the adapter's
 * `awaitChange` receives is checked to carry no `signal` (and only serializable
 * scalars). As a secondary in-process approximation of the DO RPC boundary we also
 * `structuredClone` each argument; note this is weaker than real workerd RPC (e.g.
 * Node's `structuredClone(new AbortController().signal)` does not throw), so the
 * `not.toHaveProperty("signal")` check below is the real guard. The deployed DO
 * conformance suite covers the true serialization boundary.
 */
import { describe, expect, it, vi } from "vitest";
import { SseHttpService } from "./sse-http-service.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { HttpResponseFactory } from "../responses.ts";
import { SseEventEncoder } from "../sse-event-encoder.ts";
import { StreamProtocol } from "../../protocol.ts";
import { createMemoryStorageAdapter } from "../../storage/memory/adapter.ts";
import type { StorageAdapter } from "../../types/storage-adapter.ts";
import type { AwaitChangeOptions } from "../../types/storage.ts";

const fixedTime = new Date("2026-06-30T00:00:00.000Z").getTime();
const clock = {
  now: () => fixedTime,
  date: (value?: number | string) => new Date(value ?? fixedTime),
};

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("SSE boundary — awaitChange receives only serializable arguments", () => {
  it("never passes the caller-local AbortSignal (or any non-serializable value) to the adapter", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const seen: AwaitChangeOptions[] = [];

      const base = createMemoryStorageAdapter();
      const guard: StorageAdapter = {
        ...base,
        awaitChange(streamId, options) {
          // Reproduce the DO RPC structured-clone boundary locally: this throws
          // `DataCloneError` on an AbortSignal / function / any non-cloneable value.
          structuredClone({ streamId, options });
          seen.push(options);
          return base.awaitChange!(streamId, options);
        },
      };

      const protocol = new StreamProtocol({
        storage: { adapter: guard },
        clock,
        longPollTimeoutMs: 40,
      });
      const created = await protocol.create("s", { contentType: "text/plain" });
      expect(created.status).toBe("created");
      if (created.status !== "created") throw new Error("expected created");

      const service = new SseHttpService({
        responses: new HttpResponseFactory(),
        sseEvents: new SseEventEncoder(new MessageBodyCodec()),
        clock,
      });

      // `offset: "now"` parks the live loop at the tail, so the loop long-polls
      // through `awaitChange` (no new messages, stream open).
      const response = await service.execute(created.stream, "now");
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      await reader.read(); // initial control event

      // Allow at least one long-poll (40ms) cycle through awaitChange.
      await delay(140);
      await reader.cancel();

      expect(seen.length).toBeGreaterThan(0);
      for (const options of seen) {
        expect(options).not.toHaveProperty("signal");
        expect(typeof options.timeoutMs).toBe("number");
        expect(typeof options.fromOffset).toBe("string");
      }
      // A structured-clone failure would have rejected the live read and logged an
      // "SSE stream error"; the clean run proves no non-serializable value crossed.
      expect(consoleError).not.toHaveBeenCalled();
    } finally {
      consoleError.mockRestore();
    }
  });
});
