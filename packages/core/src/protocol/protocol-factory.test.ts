import { describe, expect, it } from "vitest";
import { ProtocolStream, StreamProtocol } from "../protocol.ts";
import { createMemoryStorageAdapter } from "../storage/memory/adapter.ts";
import type { AwaitChangeOptions, AwaitChangeResult } from "../types/storage.ts";

function timeoutResult(options: AwaitChangeOptions): AwaitChangeResult {
  return {
    status: "timeout",
    snapshot: {
      present: true,
      currentOffset: options.fromOffset,
      closed: options.observedClosed ?? false,
      softDeleted: options.observedSoftDeleted ?? false,
    },
  };
}

describe("StreamProtocol factory", () => {
  it("uses a 30 second long-poll timeout by default", async () => {
    const timeouts: number[] = [];
    const base = createMemoryStorageAdapter();
    const protocol = new StreamProtocol({
      storage: {
        adapter: {
          ...base,
          awaitChange: async (_streamId, options) => {
            timeouts.push(options.timeoutMs);
            return timeoutResult(options);
          },
        },
      },
    });
    const created = await protocol.create("default-timeout", { contentType: "text/plain" });
    if (created.status !== "created") throw new Error("expected create");

    const result = await created.stream.readLive({ offset: "0", mode: "long-poll" });

    expect(result.status).toBe("timeout");
    expect(timeouts).toEqual([30_000]);
  });

  it("creates and then resolves a bound protocol stream", async () => {
    const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const created = await protocol.create("alpha", {
      contentType: "text/plain",
      initialData: new TextEncoder().encode("hello"),
    });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");
    expect(created.stream).toBeInstanceOf(ProtocolStream);
    expect(created.stream.id).toBe("alpha");

    const lookup = await protocol.get("alpha");
    expect(lookup.status).toBe("ok");
    if (lookup.status !== "ok") throw new Error("expected lookup");
    expect(lookup.stream).toBeInstanceOf(ProtocolStream);
    const read = await lookup.stream.read({});
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected read");
    expect(new TextDecoder().decode(read.messages[0]!.data)).toBe("hello");
  });
});
