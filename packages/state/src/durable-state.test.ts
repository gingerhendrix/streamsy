import { describe, expect, it } from "vitest";
import {
  createMemoryStreamFactory,
  createStreamProtocol,
  type StreamProtocolFactory,
} from "@streamsy/core";
import {
  createDurableStateProtocol,
  type DurableStateMessage,
  type JsonCodec,
  type ValuesByWireType,
} from "./index.ts";

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
  return createStreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
}

describe("DurableStateProtocol", () => {
  it("emits standards-shaped change and control messages", async () => {
    const protocol = createProtocol();
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
      { headers: { control: "snapshot-end" } },
      { headers: { control: "reset" } },
    ]);
  });

  it("rejects values that fail schema validation before appending", async () => {
    const protocol = createProtocol();
    const durable = createDurableStateProtocol(protocol, {
      users: { type: "user", schema: userCodec, primaryKey: "id" },
    });

    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    expect(() => created.stream.state.insert("users", { id: "u1" } as unknown as User)).toThrow(
      "invalid user",
    );

    const read = await created.stream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages).toEqual([]);
  });

  it("infers keys via a function primaryKey", async () => {
    const protocol = createProtocol();
    const durable = createDurableStateProtocol(protocol, {
      users: {
        schema: userCodec,
        primaryKey: (value: unknown) => `user:${(value as User).id}`,
      },
    });

    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    await created.stream.state.insert("users", { id: "u1", name: "Alice" });

    const read = await created.stream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages).toHaveLength(1);
    const message = read.messages[0]!.value;
    if (!("type" in message)) throw new Error("expected change message");
    expect(message.key).toBe("user:u1");
  });

  it("rejects unknown collection types", async () => {
    const protocol = createProtocol();
    const schema = {
      users: { type: "user", schema: userCodec, primaryKey: "id" },
    } as const;
    const durable = createDurableStateProtocol(protocol, schema);

    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    const ghost = {
      type: "ghost",
      key: "g1",
      value: { id: "g1", name: "Ghost" },
      headers: { operation: "insert" },
    } as unknown as DurableStateMessage<ValuesByWireType<typeof schema>>;
    expect(() => created.stream.state.append(ghost)).toThrow("Unknown Durable State type: ghost");

    const read = await created.stream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages).toEqual([]);
  });
});
