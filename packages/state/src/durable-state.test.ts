import { describe, expect, it } from "vitest";
import {
  createMemoryStorageAdapter,
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

type UserSourceValue = {} | null | undefined;

interface UserCandidate {
  readonly id?: UserSourceValue;
  readonly name?: UserSourceValue;
}

function isUserCandidate(value: UserSourceValue): value is UserCandidate {
  return value !== null && Object(value) === value && !Array.isArray(value);
}

function isStringValue(value: UserSourceValue): value is string {
  return (
    value !== null && value !== undefined && Object(value) !== value && value.constructor === String
  );
}

function parseUser(value: UserSourceValue): User {
  if (!isUserCandidate(value)) throw new Error("invalid user");
  const { id, name } = value;
  if (!isStringValue(id) || !isStringValue(name)) throw new Error("invalid user");
  return { id, name };
}

const userCodec: JsonCodec<User> = {
  encode: (value) => value,
  decode: parseUser,
};

/**
 * Declares a wider value type than `userCodec` accepts at runtime, so a
 * well-typed caller can still submit a value the schema rejects. Validation is
 * delegated to `userCodec`, so the runtime contract is unchanged.
 */
const partialUserCodec: JsonCodec<Partial<User>> = {
  encode: (value) => value,
  decode: (value) => userCodec.decode(value),
};

function createProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
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
    await created.stream.state.upsert("users", { id: "u2", name: "Bob" });
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
      {
        type: "user",
        key: "u2",
        value: { id: "u2", name: "Bob" },
        headers: { operation: "upsert" },
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
      users: { type: "user", schema: partialUserCodec, primaryKey: "id" },
    });

    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    expect(() => created.stream.state.insert("users", { id: "u1" })).toThrow("invalid user");

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
        primaryKey: (value: User) => `user:${value.id}`,
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
    // Without `as const` the collection's `type` is `string`, so the wire-type
    // map is open and an unknown tag is a statically valid message. Rejecting it
    // is then purely the runtime lookup's job.
    const schema = {
      users: { type: "user", schema: userCodec, primaryKey: "id" },
    };
    const durable = createDurableStateProtocol(protocol, schema);

    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    const ghost: DurableStateMessage<ValuesByWireType<typeof schema>> = {
      type: "ghost",
      key: "g1",
      value: { id: "g1", name: "Ghost" },
      headers: { operation: "insert" },
    };
    expect(() => created.stream.state.append(ghost)).toThrow("Unknown Durable State type: ghost");

    const read = await created.stream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages).toEqual([]);
  });

  it("round-trips change and control messages through a reopened stream", async () => {
    const protocol = createProtocol();
    const durable = createDurableStateProtocol(protocol, {
      users: { type: "user", schema: userCodec, primaryKey: "id" },
    });

    const created = await durable.create("state");
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected created");

    await created.stream.state.insert("users", { id: "u1", name: "Alice" });
    await created.stream.state.snapshotEnd();

    const reopened = await durable.get("state");
    expect(reopened.status).toBe("ok");
    if (reopened.status !== "ok") throw new Error("expected ok");

    const read = await reopened.stream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok");
    expect(read.messages).toHaveLength(2);

    const change = read.messages[0]!.value;
    if (!("type" in change)) throw new Error("expected change message");
    expect(change.type).toBe("user");
    expect(change.key).toBe("u1");
    expect(change.value).toEqual({ id: "u1", name: "Alice" });

    const control = read.messages[1]!.value;
    if ("type" in control) throw new Error("expected control message");
    expect(control.headers).toEqual({ control: "snapshot-end" });
  });

  it("rejects malformed stored messages on read", async () => {
    const protocol = createProtocol();
    const durable = createDurableStateProtocol(protocol, {
      users: { type: "user", schema: userCodec, primaryKey: "id" },
    });

    const cases: ReadonlyArray<{ name: string; payload: unknown; message: string }> = [
      {
        name: "unknown wire tag",
        payload: {
          type: "ghost",
          key: "g1",
          value: { id: "g1", name: "Ghost" },
          headers: { operation: "insert" },
        },
        message: "Unknown Durable State type: ghost",
      },
      {
        name: "payload rejected by the collection schema",
        payload: { type: "user", key: "u1", value: { id: "u1" }, headers: { operation: "insert" } },
        message: "invalid user",
      },
      {
        name: "missing headers",
        payload: { type: "user", key: "u1", value: { id: "u1", name: "Alice" } },
        message: "Durable State message requires headers object",
      },
      {
        name: "unknown operation",
        payload: {
          type: "user",
          key: "u1",
          value: { id: "u1", name: "Alice" },
          headers: { operation: "patch" },
        },
        message: "Invalid Durable State operation",
      },
      {
        name: "unknown control",
        payload: { headers: { control: "rewind" } },
        message: "Invalid Durable State control message",
      },
    ];

    for (const [index, testCase] of cases.entries()) {
      // `appendJson` bypasses the Durable State codec, so each case stores bytes
      // that only the read path can reject. One stream per case: the first bad
      // message ends the read.
      const created = await durable.create(`state-${index}`);
      expect(created.status, testCase.name).toBe("created");
      if (created.status !== "created") throw new Error("expected created");
      await created.stream.json.appendJson(testCase.payload);

      const read = await created.stream.read();
      expect(read.status, testCase.name).toBe("invalid-json");
      if (read.status !== "invalid-json") throw new Error("expected invalid-json");
      expect(String(read.error), testCase.name).toContain(testCase.message);
    }
  });
});
