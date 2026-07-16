import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  directProtocolClient,
} from "@streamsy/core";
import { describe, expect, test } from "vitest";
import { materialize, type Materializer } from "./materialize.ts";
import { streamCheckpointStore } from "./stream-checkpoint-store.ts";

const encoder = new TextEncoder();
const sum: Materializer<number, number> = {
  initial: () => 0,
  evolve: (state, event) => state + event,
};

const decodeNumbers = (batch: { kind: string; items?: readonly unknown[] }): Iterable<number> => {
  if (batch.kind !== "json") throw new Error(`expected json batch, received ${batch.kind}`);
  return batch.items as readonly number[];
};

describe("streamCheckpointStore", () => {
  test("roundtrips checkpoints and latest record wins", async () => {
    const protocol = createStreamProtocol({
      storage: { adapter: createMemoryStorageAdapter() },
    });
    const store = streamCheckpointStore<number>({ client: directProtocolClient(protocol) });
    expect(await store.load("totals")).toBeNull();

    await store.save("totals", {
      cursors: { numbers: "0000000000000001_0000000000000000" },
      snapshot: 1,
    });
    const latest = {
      cursors: { numbers: "0000000000000002_0000000000000000" },
      appliedThrough: "0000000000000002_0000000000000000",
      snapshot: 3,
    };
    await store.save("totals", latest);
    expect(await store.load("totals")).toEqual(latest);
  });

  test("fails fast with view context when checkpoint JSON is malformed", async () => {
    const protocol = createStreamProtocol({
      storage: { adapter: createMemoryStorageAdapter() },
    });
    const created = await protocol.create("__streamsy/views/bad%2Fview/checkpoint", {
      contentType: "text/plain",
    });
    if (created.status !== "created") throw new Error(`create failed: ${created.status}`);
    const appended = await created.stream.append({
      contentType: "text/plain",
      data: encoder.encode("{not-json"),
    });
    if (appended.status !== "appended") throw new Error(`append failed: ${appended.status}`);

    await expect(
      streamCheckpointStore<number>({ client: directProtocolClient(protocol) }).load("bad/view"),
    ).rejects.toThrow("Cannot load checkpoint for bad/view: latest record is malformed");
  });

  test("a recreated client loads a checkpoint and resumes without replaying it", async () => {
    const adapter = createMemoryStorageAdapter();
    let protocol = createStreamProtocol({ storage: { adapter } });
    const created = await protocol.create("numbers", {
      contentType: "application/json",
    });
    if (created.status !== "created") throw new Error(`create failed: ${created.status}`);
    const first = await created.stream.append({
      contentType: "application/json",
      data: encoder.encode("2"),
    });
    if (first.status !== "appended") throw new Error(`append failed: ${first.status}`);

    let client = directProtocolClient(protocol);
    const initial = await materialize({
      source: { client, streamId: "numbers" },
      decode: decodeNumbers,
      view: sum,
    });
    await streamCheckpointStore<number>({ client }).save("totals", {
      cursors: { numbers: initial.cursor },
      snapshot: initial.state,
    });

    protocol = createStreamProtocol({ storage: { adapter } });
    const lookup = await protocol.get("numbers");
    if (lookup.status !== "ok") throw new Error(`get failed: ${lookup.status}`);
    const second = await lookup.stream.append({
      contentType: "application/json",
      data: encoder.encode("3"),
    });
    if (second.status !== "appended") throw new Error(`append failed: ${second.status}`);

    client = directProtocolClient(protocol);
    const checkpoint = await streamCheckpointStore<number>({ client }).load("totals");
    expect(checkpoint).not.toBeNull();
    const resumed = await materialize({
      source: { client, streamId: "numbers" },
      decode: decodeNumbers,
      view: sum,
      from: checkpoint?.cursors.numbers,
      initialState: checkpoint?.snapshot,
    });
    expect(resumed).toEqual({ state: 5, cursor: second.offset });
  });
});
