import { describe, expect, it } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  ZERO_OFFSET,
  type StreamProtocolFactory,
} from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "./index.ts";

const decoder = new TextDecoder();
const encoder = new TextEncoder();

type User = { id: string; name: string };

const userCodec: JsonCodec<User> = {
  encode(value) {
    if (typeof value.id !== "string" || typeof value.name !== "string")
      throw new Error("invalid user");
    return value;
  },
  decode(value) {
    if (!value || typeof value !== "object") throw new Error("invalid user");
    const candidate = value as Partial<User>;
    if (typeof candidate.id !== "string" || typeof candidate.name !== "string") {
      throw new Error("invalid user");
    }
    return { id: candidate.id, name: candidate.name };
  },
};

function createProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
}

describe("JsonProtocol", () => {
  it("wraps the underlying protocol and reads typed JSON values", async () => {
    const protocol = createProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    expect(json.protocol).toBe(protocol);
    const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });

    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");
    expect(created.contentType).toBe("application/json");

    await created.stream.append({ id: "u2", name: "Bob" }, { seq: "bob" });
    const read = await created.stream.read();

    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages.map((message) => message.value.name)).toEqual(["Alice", "Bob"]);
  });

  it("encodes values as JSON bytes on the underlying stream", async () => {
    const protocol = createProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });
    expect(created.status).toBe("created");

    const lookup = await protocol.get("users");
    expect(lookup.status).toBe("ok");
    if (lookup.status !== "ok") throw new Error("expected ok");
    const raw = await lookup.stream.read({});
    expect(raw.status).toBe("ok");
    if (raw.status !== "ok") throw new Error("expected ok");
    expect(raw.messages).toHaveLength(1);
    expect(decoder.decode(raw.messages[0]!.data)).toBe('{"id":"u1","name":"Alice"}');
  });

  it("rejects non-json streams on get", async () => {
    const protocol = createProtocol();
    await protocol.create("raw", { contentType: "text/plain" });

    const result = await createJsonProtocol(protocol, userCodec).get("raw");

    expect(result).toEqual({
      status: "content-type-conflict",
      contentType: "text/plain",
      expectedContentType: "application/json",
    });
  });

  it("propagates JSON parse errors when invalid bytes are appended to a json stream", async () => {
    // The base protocol's message framer parses application/json bodies, so
    // invalid JSON bytes are rejected at append time and never reach storage.
    // The read-side invalid-json status therefore only surfaces codec/schema
    // failures (covered below) or externally corrupted storage.
    const protocol = createProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    const created = await json.create("users");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    const lookup = await protocol.get("users");
    expect(lookup.status).toBe("ok");
    if (lookup.status !== "ok") throw new Error("expected ok");
    await expect(() =>
      lookup.stream.append({
        data: encoder.encode("not json"),
        contentType: "application/json",
      }),
    ).rejects.toThrow(SyntaxError);
  });

  it("returns invalid-json when a stored value fails schema validation", async () => {
    const protocol = createProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    const created = await json.create("users");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    await created.stream.appendJson({ wrong: "shape" });

    const read = await created.stream.read();
    expect(read.status).toBe("invalid-json");
    if (read.status !== "invalid-json") throw new Error("expected invalid-json");
    expect((read.error as Error).message).toBe("invalid user");
  });

  it("reads typed messages through readLive", async () => {
    const protocol = createProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    const live = await created.stream.readLive({ offset: ZERO_OFFSET, mode: "long-poll" });
    expect(live.status).toBe("ok");
    if (live.status !== "ok") throw new Error("expected ok");
    expect(live.messages.map((message) => message.value)).toEqual([{ id: "u1", name: "Alice" }]);
  });
});
