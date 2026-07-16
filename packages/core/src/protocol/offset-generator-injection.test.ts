import { describe, expect, it } from "vitest";
import { HttpHandler } from "../http.ts";
import { StreamProtocol } from "../protocol.ts";
import { createMemoryStorageAdapter } from "../storage/memory/adapter.ts";
import { InvalidGeneratedOffsetError, type OffsetGenerator } from "./helpers/offset-generator.ts";

const enc = new TextEncoder();

function monotonicTokens(): OffsetGenerator {
  const initialOffset = "00000000000000000000000000";
  return {
    initialOffset,
    isValid: (offset) => /^[0-9A-Z]{26}$/.test(offset),
    next: (previous) => {
      const value = BigInt(`0x${BigInt(parseInt(previous.slice(-6), 36)).toString(16)}`) + 1n;
      return `${previous.slice(0, -6)}${value.toString(36).toUpperCase().padStart(6, "0")}`;
    },
  };
}

function protocolWith(offsetGenerator: OffsetGenerator) {
  return new StreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
    offsetGenerator,
  });
}

describe("offset generator injection", () => {
  it("uses opaque custom offsets for create batches, appends, and fork continuation", async () => {
    const offsets = monotonicTokens();
    const protocol = protocolWith(offsets);
    const created = await protocol.create("source", {
      contentType: "application/json",
      initialData: enc.encode('[{"n":1},{"n":2}]'),
    });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");
    expect(created.nextOffset).toBe("00000000000000000000000002");

    const appended = await created.stream.append({
      contentType: "application/json",
      data: enc.encode('[{"n":3},{"n":4}]'),
    });
    expect(appended).toMatchObject({
      status: "appended",
      offset: "00000000000000000000000004",
    });

    const fork = await protocol.create("child", {
      forkedFrom: "source",
      forkOffset: "00000000000000000000000002",
      initialData: enc.encode('{"child":true}'),
    });
    expect(fork.status).toBe("created");
    if (fork.status !== "created") throw new Error("expected fork");
    expect(fork.nextOffset).toBe("00000000000000000000000003");

    const read = await fork.stream.read({ offset: offsets.initialOffset });
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected read");
    expect(read.messages.map((message) => message.offset)).toEqual([
      "00000000000000000000000001",
      "00000000000000000000000002",
      "00000000000000000000000003",
    ]);
  });

  it("rejects a non-increasing generated value before committing it", async () => {
    const initialOffset = "00000000000000000000000000";
    const protocol = protocolWith({
      initialOffset,
      isValid: () => true,
      next: () => initialOffset,
    });
    const created = await protocol.create("broken", { contentType: "text/plain" });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");

    await expect(
      created.stream.append({ contentType: "text/plain", data: enc.encode("x") }),
    ).rejects.toBeInstanceOf(InvalidGeneratedOffsetError);
    const metadata = await created.stream.metadata();
    expect(metadata).toMatchObject({ status: "ok", nextOffset: initialOffset });
  });

  it("routes HTTP read and expected-offset validation through the custom scheme", async () => {
    const offsets = monotonicTokens();
    const protocol = protocolWith(offsets);
    const handler = new HttpHandler({ protocol });
    await handler.fetch(
      new Request("http://x/custom", {
        method: "PUT",
        headers: { "content-type": "text/plain" },
        body: "a",
      }),
    );

    expect((await handler.fetch(new Request("http://x/custom?offset=1_0"))).status).toBe(400);
    expect(
      (
        await handler.fetch(
          new Request("http://x/custom", {
            method: "POST",
            headers: {
              "content-type": "text/plain",
              "stream-expected-offset": "1_0",
            },
            body: "b",
          }),
        )
      ).status,
    ).toBe(400);
    expect(
      (await handler.fetch(new Request(`http://x/custom?offset=${offsets.initialOffset}`))).status,
    ).toBe(200);
  });

  it("rejects the short default cursor that previously skipped canonical data", async () => {
    const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const handler = new HttpHandler({ protocol });
    const create = await handler.fetch(
      new Request("http://x/default", {
        method: "PUT",
        headers: { "content-type": "text/plain" },
        body: "unread",
      }),
    );
    expect(create.status).toBe(201);

    const skipped = await handler.fetch(new Request("http://x/default?offset=1_0"));
    expect(skipped.status).toBe(400);
    expect(await skipped.text()).toBe("Invalid offset format");
  });
});
