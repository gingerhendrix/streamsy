import {
  StreamProtocol,
  createMemoryStorageAdapter,
  directProtocolClient,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { Cause, Effect, Exit, Option } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity, type StreamIdentity } from "../causal.ts";
import { ProjectionPoison } from "../effect/errors.ts";
import { AppendStreamsLive, ReadStreamsLive } from "../effect/streams.ts";
import {
  catchUpDynamicFanInState,
  FanInRecoveryLive,
  type CatchUpFanInResult,
  type MembershipChange,
} from "./fan-in-state.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";

const clients = new Set<StreamProtocolClient>();
const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

/** Board state keyed by member name; the value is the last observed number. */
type Board = Readonly<Record<string, number>>;

interface Harness {
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly membership: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly member: (name: string) => StreamBinding;
  readonly createMember: (name: string) => Promise<void>;
}

async function harness(
  adapter: StorageAdapter = createMemoryStorageAdapter(),
  options: { readonly missingMemberStreams?: boolean } = {},
): Promise<Harness> {
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  clients.add(client);
  const membershipIdentity = streamIdentity("board-membership");
  const targetIdentity = streamIdentity("board-state");
  const membership = bindStream({
    identity: membershipIdentity,
    client,
    streamId: "board-membership",
  });
  const target = bindStream({ identity: targetIdentity, client, streamId: "board-state" });
  const lane = await deriveProducerLane({
    processorId: "board",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: membershipIdentity,
    target: targetIdentity,
    producerEpoch: 5,
  });
  await client.stream(membership.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });
  const known = new Set<string>();
  return {
    adapter,
    client,
    membership,
    target,
    lane,
    member: (name) => bindStream({ identity: streamIdentity(name), client, streamId: name }),
    async createMember(name) {
      if (options.missingMemberStreams || known.has(name)) return;
      known.add(name);
      await client.stream(name).create({ contentType: "application/json" });
    },
  };
}

function program(
  h: Harness,
  options: {
    readonly resolvable?: readonly string[];
    readonly limits?: typeof limits;
    readonly poisonMember?: boolean;
  } = {},
) {
  return catchUpDynamicFanInState<Board, number>({
    membership: h.membership,
    target: h.target,
    lane: h.lane,
    limits: options.limits ?? limits,
    initial: {},
    restore(initial, events) {
      let board: Board = initial;
      for (const event of events) {
        if (!isRecord(event) || event.type !== "row" || typeof event.key !== "string") {
          throw new TypeError("unexpected board fact");
        }
        if (isRecord(event.headers) && event.headers.operation === "delete") {
          const { [event.key]: _removed, ...rest } = board;
          board = rest;
          continue;
        }
        const value = event.value;
        if (!isRecord(value) || typeof value.last !== "number") {
          throw new TypeError("malformed board row");
        }
        board = { ...board, [event.key]: value.last };
      }
      return board;
    },
    decodeMembership(batch) {
      if (batch.kind !== "json") throw new TypeError("expected JSON membership");
      return batch.items.map((item): MembershipChange => {
        if (!isRecord(item) || typeof item.member !== "string") {
          throw new TypeError("bad membership fact");
        }
        if (item.type === "leave") {
          return { type: "leave", member: streamIdentity(item.member) };
        }
        return {
          type: "join",
          member: streamIdentity(item.member),
          ...(typeof item.from === "string" ? { from: item.from } : {}),
        };
      });
    },
    resolveMember(identity: StreamIdentity) {
      if (options.resolvable && !options.resolvable.includes(identity.name)) return undefined;
      return h.member(identity.name);
    },
    decodeMember(batch) {
      if (options.poisonMember) throw new Error("poisoned member decode");
      if (batch.kind !== "json") throw new TypeError("expected JSON member");
      return batch.items.map((item) => {
        if (!isRecord(item) || typeof item.v !== "number") throw new TypeError("bad member item");
        return item.v;
      });
    },
    onRecord(state, member, values) {
      const last = values.at(-1);
      if (last === undefined) return { state, facts: [] };
      return {
        state: { ...state, [member.identity.name]: last },
        facts: [
          {
            type: "row",
            key: member.identity.name,
            value: { last },
            headers: { operation: "upsert" },
          },
        ],
      };
    },
    onRemove(state, member) {
      const { [member.identity.name]: _removed, ...rest } = state;
      return {
        state: rest,
        facts: [{ type: "row", key: member.identity.name, headers: { operation: "delete" } }],
      };
    },
  }).pipe(
    Effect.provide(FanInRecoveryLive),
    Effect.provide(ReadStreamsLive),
    Effect.provide(AppendStreamsLive),
  );
}

function run(
  h: Harness,
  options?: Parameters<typeof program>[1],
): Promise<CatchUpFanInResult<Board>> {
  return Effect.runPromise(program(h, options));
}

async function join(h: Harness, name: string, from?: string): Promise<void> {
  await h.createMember(name);
  await h.client
    .stream(h.membership.streamId)
    .appendJsonBatch([{ type: "join", member: name, ...(from === undefined ? {} : { from }) }]);
}

async function leave(h: Harness, name: string): Promise<void> {
  await h.client.stream(h.membership.streamId).appendJsonBatch([{ type: "leave", member: name }]);
}

async function emit(h: Harness, name: string, value: number): Promise<string> {
  await h.createMember(name);
  const result = await h.client.stream(name).appendJsonBatch([{ v: value }]);
  if (result.status !== "appended") throw new Error(`expected append, got ${result.status}`);
  return result.offset;
}

describe("catchUpDynamicFanInState — deterministic dynamic fan-in", () => {
  test("a joined member is incorporated and its cursor survives restart", async () => {
    const adapter = createMemoryStorageAdapter();
    const h = await harness(adapter);
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    const first = await run(h);
    expect(first).toMatchObject({ status: "caught-up", state: { alpha: 1 } });

    await h.client.close();
    clients.delete(h.client);
    const restarted = await harness(adapter);
    await emit(restarted, "alpha", 2);
    const second = await run(restarted);
    expect(second).toMatchObject({ status: "caught-up", state: { alpha: 2 } });
    if (second.status !== "caught-up") throw new Error("expected caught-up");
    // Exactly one further boundary was incorporated, so the restored member
    // cursor did not replay the first record.
    expect(second.batches).toBe(1);
    expect(second.members).toHaveLength(1);
  });

  test("a join starts at the position declared by the membership fact", async () => {
    const h = await harness();
    const skipped = await emit(h, "alpha", 10);
    await emit(h, "alpha", 20);
    await join(h, "alpha", skipped);
    const result = await run(h);
    // The record at `skipped` is never incorporated; only what follows it is.
    expect(result).toMatchObject({ status: "caught-up", state: { alpha: 20 } });
  });

  test("leaving removes the row, the durable membership, and stays converged", async () => {
    const h = await harness();
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    await run(h);
    await leave(h, "alpha");
    const result = await run(h);
    expect(result).toMatchObject({ status: "caught-up", state: {} });
    if (result.status !== "caught-up") throw new Error("expected caught-up");
    expect(result.members).toHaveLength(0);
    // A later record from the removed member cannot resurrect its row.
    await emit(h, "alpha", 9);
    expect(await run(h)).toMatchObject({ status: "caught-up", state: {} });
  });

  test("two ready members are selected in canonical order after restart", async () => {
    const adapter = createMemoryStorageAdapter();
    const h = await harness(adapter);
    await join(h, "zulu");
    await join(h, "alpha");
    await emit(h, "zulu", 1);
    await emit(h, "alpha", 2);
    await run(h, { limits: { ...limits, maxBatches: 3 } });
    const firstOrder = await boardFactOrder(h);

    const other = createMemoryStorageAdapter();
    const rerun = await harness(other);
    await join(rerun, "zulu");
    await join(rerun, "alpha");
    await emit(rerun, "alpha", 2);
    await emit(rerun, "zulu", 1);
    await run(rerun, { limits: { ...limits, maxBatches: 3 } });

    // Arrival order differs between the two runs; canonical identity order does not.
    expect(await boardFactOrder(rerun)).toEqual(firstOrder);
    expect(firstOrder).toEqual(["alpha", "zulu"]);
  });

  test("a repeated pass is idempotent and a lost wake converges through repair", async () => {
    const h = await harness();
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    await run(h);
    const stored = await h.adapter.listMessages(h.target.streamId);
    expect(await run(h)).toMatchObject({ status: "caught-up", batches: 0 });
    expect(await h.adapter.listMessages(h.target.streamId)).toEqual(stored);

    // No wake is delivered for this record; the next bounded pass repairs it.
    await emit(h, "alpha", 42);
    expect(await run(h)).toMatchObject({ status: "caught-up", state: { alpha: 42 } });
  });

  test("an unresolvable member binding is an explicit status, not a silent skip", async () => {
    const h = await harness();
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    const result = await run(h, { resolvable: [] });
    expect(result).toMatchObject({ status: "unknown-member", member: "alpha" });
  });

  test("a member decode fault is typed poison and does not advance the cursor", async () => {
    const h = await harness();
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    await run(h);
    const stored = await h.adapter.listMessages(h.target.streamId);
    await emit(h, "alpha", 2);
    const exit = await Effect.runPromiseExit(program(h, { poisonMember: true }));
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(error) && error.value instanceof ProjectionPoison).toBe(true);
    }
    expect(await h.adapter.listMessages(h.target.streamId)).toEqual(stored);
    // The poison is recoverable once decoding works again.
    expect(await run(h)).toMatchObject({ status: "caught-up", state: { alpha: 2 } });
  });

  test("bounded limits stop the pass without losing durable progress", async () => {
    const h = await harness();
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    await emit(h, "alpha", 2);
    const limited = await run(h, { limits: { ...limits, maxBatches: 2 } });
    expect(limited).toMatchObject({ status: "limit-reached", limit: "maxBatches" });
    // The membership boundary and the first member boundary are already durable.
    expect(await run(h)).toMatchObject({ status: "caught-up", state: { alpha: 2 } });
  });

  test("a missing membership source is an explicit status", async () => {
    const h = await harness();
    const orphan = bindStream({
      identity: h.membership.identity,
      client: h.client,
      streamId: "absent-membership",
    });
    const result = await Effect.runPromise(
      program({ ...h, membership: orphan } as Harness).pipe(Effect.orDie),
    );
    expect(result).toMatchObject({ status: "missing", stream: "membership" });
  });

  test("a competing target writer cannot silently advance the fan-in checkpoint", async () => {
    const h = await harness();
    await join(h, "alpha");
    await emit(h, "alpha", 1);
    await run(h);
    await h.client
      .stream(h.target.streamId)
      .appendJsonBatch([
        { type: "row", key: "foreign", value: { last: 0 }, headers: { operation: "upsert" } },
      ]);
    await emit(h, "alpha", 2);
    const exit = await Effect.runPromiseExit(program(h));
    expect(Exit.isFailure(exit)).toBe(true);
  });
});

async function boardFactOrder(h: Harness): Promise<string[]> {
  const decoder = new TextDecoder();
  const keys: string[] = [];
  for (const message of await h.adapter.listMessages(h.target.streamId)) {
    const value: unknown = JSON.parse(decoder.decode(message.data));
    if (isRecord(value) && value.type === "row" && typeof value.key === "string") {
      keys.push(value.key);
    }
  }
  return keys;
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
