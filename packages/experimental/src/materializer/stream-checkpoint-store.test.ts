import { createMemoryStorageAdapter, createStreamProtocol, ZERO_OFFSET } from "@streamsy/core";
import { describe, expect, test } from "vitest";
import { materialize, type Materializer } from "./materialize.ts";
import { streamCheckpointStore } from "./stream-checkpoint-store.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const sum: Materializer<number, number> = {
  initial: () => 0,
  evolve: (state, event) => state + event,
};

describe("streamCheckpointStore", () => {
  test("roundtrips checkpoints and latest record wins", async () => {
    const protocol = createStreamProtocol({
      storage: { adapter: createMemoryStorageAdapter() },
    });
    const store = streamCheckpointStore<number>({ protocol });
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

  test("fails fast with view context when the latest record is malformed", async () => {
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

    await expect(streamCheckpointStore<number>({ protocol }).load("bad/view")).rejects.toThrow(
      "Cannot load checkpoint for bad/view: latest record is malformed",
    );
  });

  test("a recreated protocol loads a checkpoint and resumes without replaying it", async () => {
    const adapter = createMemoryStorageAdapter();
    let protocol = createStreamProtocol({ storage: { adapter } });
    const created = await protocol.create("numbers", {
      contentType: "text/plain",
    });
    if (created.status !== "created") throw new Error(`create failed: ${created.status}`);
    const first = await created.stream.append({
      contentType: "text/plain",
      data: encoder.encode("2"),
    });
    if (first.status !== "appended") throw new Error(`append failed: ${first.status}`);

    const initial = await materialize({
      source: { protocol, streamId: "numbers" },
      decode: (message) => Number(decoder.decode(message.data)),
      view: sum,
    });
    await streamCheckpointStore<number>({ protocol }).save("totals", {
      cursors: { numbers: initial.cursor },
      snapshot: initial.state,
    });

    protocol = createStreamProtocol({ storage: { adapter } });
    const lookup = await protocol.get("numbers");
    if (lookup.status !== "ok") throw new Error(`get failed: ${lookup.status}`);
    const second = await lookup.stream.append({
      contentType: "text/plain",
      data: encoder.encode("3"),
    });
    if (second.status !== "appended") throw new Error(`append failed: ${second.status}`);

    const checkpoint = await streamCheckpointStore<number>({ protocol }).load("totals");
    expect(checkpoint).not.toBeNull();
    const resumed = await materialize({
      source: { protocol, streamId: "numbers" },
      decode: (message) => Number(decoder.decode(message.data)),
      view: sum,
      from: checkpoint?.cursors.numbers ?? ZERO_OFFSET,
      initialState: checkpoint?.snapshot,
    });
    expect(resumed).toEqual({ state: 5, cursor: second.offset });
  });
});
