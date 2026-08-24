import { describe, expect, it, vi } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  ZERO_OFFSET,
  type JsonValue,
  type StreamProtocolFactory,
} from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "./index.ts";

const decoder = new TextDecoder();
const encoder = new TextEncoder();

type User = { id: string; name: string };

type JsonRecord = { readonly [key: string]: JsonValue };

function isJsonRecord(value: JsonValue): value is JsonRecord {
  return value !== null && value instanceof Object && !Array.isArray(value);
}

function isJsonString(value: JsonValue | undefined): value is string {
  return value?.constructor === String;
}

function parseUser(value: JsonValue): User {
  if (!isJsonRecord(value)) throw new Error("invalid user");
  const id = value.id;
  const name = value.name;
  if (!isJsonString(id) || !isJsonString(name)) throw new Error("invalid user");
  return { id, name };
}

const userCodec: JsonCodec<User> = {
  encode(value) {
    return value;
  },
  decode: parseUser,
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

  it("returns the JSON parse error for malformed stored bytes", async () => {
    const json = createJsonProtocol(createProtocol(), userCodec);
    const stream = await json.getOrCreate("users");
    vi.spyOn(stream.stream, "read").mockResolvedValue({
      status: "ok",
      messages: [{ data: encoder.encode("not json"), offset: "1_0", timestamp: 1 }],
      nextOffset: "1_0",
      upToDate: true,
    });

    const read = await stream.read();

    expect(read.status).toBe("invalid-json");
    if (read.status !== "invalid-json") throw new Error("expected invalid-json");
    expect(read.error).toBeInstanceOf(SyntaxError);
    expect(read.offset).toBe("1_0");
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
    // `error` is `unknown`: a codec may reject with any value, so narrow before
    // reading `message` rather than asserting the shape.
    expect(read.error).toBeInstanceOf(Error);
    if (!(read.error instanceof Error)) throw new Error("expected an Error");
    expect(read.error.message).toBe("invalid user");
  });

  it("surfaces a non-Error rejection from the codec unchanged", async () => {
    const protocol = createProtocol();
    // A codec is user code and may reject with any value; `error` is `unknown`
    // precisely so the thrown value reaches the caller as-is.
    const throwingCodec: JsonCodec<User> = {
      encode: (value) => value,
      decode() {
        throw { code: "E_SCHEMA", detail: "not a user" };
      },
    };
    const json = createJsonProtocol(protocol, throwingCodec);

    const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    const read = await created.stream.read();
    expect(read.status).toBe("invalid-json");
    if (read.status !== "invalid-json") throw new Error("expected invalid-json");
    expect(read.error).not.toBeInstanceOf(Error);
    expect(read.error).toEqual({ code: "E_SCHEMA", detail: "not a user" });
  });

  it("reads typed messages through readNext", async () => {
    const protocol = createProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    const live = await created.stream.readNext({ offset: ZERO_OFFSET });
    expect(live.status).toBe("ok");
    if (live.status !== "ok") throw new Error("expected ok");
    expect(live.messages.map((message) => message.value)).toEqual([{ id: "u1", name: "Alice" }]);
  });

  it("gets or creates streams and reads their complete typed history", async () => {
    const json = createJsonProtocol(createProtocol(), userCodec);
    const stream = await json.getOrCreate("users");
    await stream.append({ id: "u1", name: "Alice" });
    await stream.append({ id: "u2", name: "Bob" });

    const reopened = await json.getOrCreate("users");
    const all = await reopened.readAll();

    expect(all.values).toEqual([
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    expect(all.messages.map((message) => message.value.id)).toEqual(["u1", "u2"]);
    expect(all.head).toBe(all.messages[1]!.offset);
    expect(all.upToDate).toBe(true);
  });

  it("appends a typed JSON batch atomically", async () => {
    const json = createJsonProtocol(createProtocol(), userCodec);
    const stream = await json.getOrCreate("users");
    const append = vi.spyOn(stream.stream, "append");
    const batch = [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ];
    const result = await stream.appendBatch(batch);

    expect(append).toHaveBeenCalledTimes(1);
    expect(JSON.parse(decoder.decode(append.mock.calls[0]![0].data))).toEqual(batch);
    expect(result.status).toBe("appended");
    if (result.status !== "appended") throw new Error("expected appended");
    const committed = await stream.readAll();
    expect(committed.values.map((user) => user.id)).toEqual(["u1", "u2"]);
    expect(committed.head).toBe(result.offset);
    expect(committed.messages.at(-1)?.offset).toBe(result.offset);

    const rejected = await stream.appendBatch(
      [
        { id: "u3", name: "Cara" },
        { id: "u4", name: "Dan" },
      ],
      { expectedOffset: ZERO_OFFSET },
    );
    expect(rejected).toMatchObject({
      status: "conflict",
      conflictReason: "expected-offset",
      offset: result.offset,
    });
    expect((await stream.readAll()).values.map((user) => user.id)).toEqual(["u1", "u2"]);
  });
});
