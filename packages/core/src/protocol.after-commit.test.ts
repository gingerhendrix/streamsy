import { describe, expect, it, vi } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  ZERO_OFFSET,
  type CommitEvent,
  type StreamProtocolFactory,
} from "./index.ts";

const encoder = new TextEncoder();

function createProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
}

function bytes(value: string): Uint8Array {
  return encoder.encode(value);
}

describe("StreamProtocol onAfterCommit", () => {
  it("fires once per successful create, append, and fork with committed offsets", async () => {
    const protocol = createProtocol();
    const events: CommitEvent[] = [];
    protocol.onAfterCommit((event) => events.push(event));

    const created = await protocol.create("source", {
      contentType: "text/plain",
      initialData: bytes("seed"),
    });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");

    const appended = await created.stream.append({
      contentType: "text/plain",
      data: bytes("next"),
    });
    expect(appended.status).toBe("appended");
    if (appended.status !== "appended") throw new Error("expected append");

    const forked = await protocol.create("child", {
      contentType: "text/plain",
      forkedFrom: "source",
      forkOffset: appended.offset,
    });
    expect(forked.status).toBe("created");
    if (forked.status !== "created") throw new Error("expected fork");

    expect(events).toEqual([
      { streamId: "source", offset: created.nextOffset, closed: false, softDeleted: false },
      { streamId: "source", offset: appended.offset, closed: false, softDeleted: false },
      { streamId: "child", offset: forked.nextOffset, closed: false, softDeleted: false },
    ]);
  });

  it("does not fire on a rejected append", async () => {
    const protocol = createProtocol();
    const created = await protocol.create("source", { contentType: "text/plain" });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");

    const events: CommitEvent[] = [];
    protocol.onAfterCommit((event) => events.push(event));

    const appended = await created.stream.append({
      contentType: "text/plain",
      data: bytes("advance"),
    });
    expect(appended.status).toBe("appended");

    const rejected = await created.stream.append({
      contentType: "text/plain",
      data: bytes("stale"),
      expectedOffset: ZERO_OFFSET,
    });

    expect(rejected).toMatchObject({ status: "conflict", conflictReason: "expected-offset" });
    expect(events).toHaveLength(1);
    if (appended.status !== "appended") throw new Error("expected append");
    expect(events[0]).toMatchObject({ streamId: "source", offset: appended.offset });
  });

  it("emits offset-only lifecycle state without message bodies", async () => {
    const protocol = createProtocol();
    const events: CommitEvent[] = [];
    protocol.onAfterCommit((event) => events.push(event));

    const created = await protocol.create("closed", {
      contentType: "text/plain",
      initialData: bytes("body"),
      closed: true,
    });

    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");
    expect(events).toEqual([
      { streamId: "closed", offset: created.nextOffset, closed: true, softDeleted: false },
    ]);
    expect(events[0]).not.toHaveProperty("data");
    expect(events[0]).not.toHaveProperty("message");
    expect(events[0]).not.toHaveProperty("messages");
    expect(events[0]).not.toHaveProperty("contentType");
  });

  it("isolates hook errors from commits and other hooks", async () => {
    const protocol = createProtocol();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const received: CommitEvent[] = [];
    protocol.onAfterCommit(() => {
      throw new Error("boom");
    });
    protocol.onAfterCommit((event) => received.push(event));

    try {
      const created = await protocol.create("source", { contentType: "text/plain" });

      expect(created.status).toBe("created");
      if (created.status !== "created") throw new Error("expected create");
      expect(received).toEqual([
        { streamId: "source", offset: created.nextOffset, closed: false, softDeleted: false },
      ]);
      expect(warn).toHaveBeenCalledTimes(1);
    } finally {
      warn.mockRestore();
    }
  });

  it("unsubscribes hooks from further delivery", async () => {
    const protocol = createProtocol();
    const events: CommitEvent[] = [];
    const unsubscribe = protocol.onAfterCommit((event) => events.push(event));

    const first = await protocol.create("one", { contentType: "text/plain" });
    expect(first.status).toBe("created");
    unsubscribe();
    const second = await protocol.create("two", { contentType: "text/plain" });
    expect(second.status).toBe("created");

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ streamId: "one" });
  });
});
