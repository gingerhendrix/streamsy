import { describe, expect, it } from "vitest";
import {
  createDurableStateProtocol,
  createJsonProtocol,
  type JsonCodec,
  type ProtocolStream,
  type StreamProtocolFactory,
} from "./index.ts";
import type {
  AppendOptions,
  CreateOptions,
  MetadataResult,
  ReadLiveOptions,
  ReadOptions,
} from "./types/protocol.ts";
import type { StoredMessage } from "./types/storage.ts";

const decoder = new TextDecoder();

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

class MemoryProtocolStream implements ProtocolStream {
  readonly id: string;
  readonly messages: StoredMessage[] = [];
  contentType = "application/octet-stream";
  closed = false;

  constructor(id: string) {
    this.id = id;
  }

  async append(options: AppendOptions) {
    if (options.contentType !== this.contentType)
      return { status: "conflict" as const, conflictReason: "content-type" as const };
    const offset = `${this.messages.length}_0`;
    this.messages.push({ data: options.data, offset, timestamp: this.messages.length });
    this.closed = options.close === true;
    return {
      status: "appended" as const,
      nextOffset: `${this.messages.length}_0`,
      closed: this.closed,
    };
  }

  async read(_options: ReadOptions) {
    return {
      status: "ok" as const,
      messages: this.messages,
      nextOffset: `${this.messages.length}_0`,
      upToDate: true,
      closed: this.closed,
    };
  }

  async readLive(options: ReadLiveOptions) {
    return {
      status: "ok" as const,
      messages: this.messages,
      nextOffset: `${this.messages.length}_0`,
      upToDate: true,
      cursor: options.cursor ?? "cursor",
      closed: this.closed,
    };
  }

  async metadata(): Promise<MetadataResult> {
    return {
      status: "ok",
      contentType: this.contentType,
      nextOffset: `${this.messages.length}_0`,
      closed: this.closed,
    };
  }

  async delete() {
    return { status: "ok" as const };
  }
}

class MemoryProtocol implements StreamProtocolFactory {
  readonly streams = new Map<string, MemoryProtocolStream>();

  async create(streamId: string, options: CreateOptions) {
    let stream = this.streams.get(streamId);
    const status = stream ? "exists" : "created";
    if (!stream) {
      stream = new MemoryProtocolStream(streamId);
      stream.contentType = options.contentType ?? "application/octet-stream";
      this.streams.set(streamId, stream);
      if (options.initialData)
        await stream.append({ data: options.initialData, contentType: stream.contentType });
    }
    return {
      status,
      stream,
      nextOffset: `${stream.messages.length}_0`,
      contentType: stream.contentType,
      closed: stream.closed,
    } as const;
  }

  async get(streamId: string) {
    const stream = this.streams.get(streamId);
    if (!stream) return { status: "not-found" as const };
    return { status: "ok" as const, stream };
  }
}

describe("JsonProtocol", () => {
  it("wraps the underlying protocol and reads typed JSON values", async () => {
    const protocol = new MemoryProtocol();
    const json = createJsonProtocol(protocol, userCodec);

    expect(json.protocol).toBe(protocol);
    const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });

    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");
    expect(created.contentType).toBe("application/json");
    expect(decoder.decode(protocol.streams.get("users")!.messages[0]!.data)).toBe(
      '{"id":"u1","name":"Alice"}',
    );

    await created.stream.append({ id: "u2", name: "Bob" }, { seq: "bob" });
    const read = await created.stream.read();

    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages.map((message) => message.value.name)).toEqual(["Alice", "Bob"]);
  });

  it("rejects non-json streams on get", async () => {
    const protocol = new MemoryProtocol();
    await protocol.create("raw", { contentType: "text/plain" });

    const result = await createJsonProtocol(protocol, userCodec).get("raw");

    expect(result).toEqual({
      status: "content-type-conflict",
      contentType: "text/plain",
      expectedContentType: "application/json",
    });
  });
});

describe("DurableStateProtocol", () => {
  it("emits standards-shaped change and control messages", async () => {
    const protocol = new MemoryProtocol();
    const durable = createDurableStateProtocol(protocol, {
      users: { type: "user", schema: userCodec, primaryKey: "id" },
    });

    expect(durable.protocol).toBe(protocol);
    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    await created.stream.state.insert(
      "users",
      { id: "u1", name: "Alice" },
      { headers: { txid: "t1" } },
    );
    await created.stream.state.update(
      "users",
      { id: "u1", name: "Alicia" },
      { oldValue: { id: "u1", name: "Alice" } },
    );
    await created.stream.state.delete("users", "u1");
    await created.stream.state.snapshotStart({ offset: "2_0" });
    await created.stream.state.snapshotEnd();
    await created.stream.state.reset();

    const read = await created.stream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages.map((message) => message.value)).toEqual([
      {
        type: "user",
        key: "u1",
        value: { id: "u1", name: "Alice" },
        headers: { txid: "t1", operation: "insert" },
      },
      {
        type: "user",
        key: "u1",
        value: { id: "u1", name: "Alicia" },
        old_value: { id: "u1", name: "Alice" },
        headers: { operation: "update" },
      },
      { type: "user", key: "u1", headers: { operation: "delete" } },
      { headers: { offset: "2_0", control: "snapshot-start" } },
      { headers: { offset: undefined, control: "snapshot-end" } },
      { headers: { offset: undefined, control: "reset" } },
    ]);
  });
});
