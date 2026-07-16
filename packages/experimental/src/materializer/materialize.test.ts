import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  directProtocolClient,
} from "@streamsy/core";
import type { JsonValue, StreamProtocolClient } from "@streamsy/core";
import { describe, expect, test } from "vitest";
import { materialize, type Materializer } from "./materialize.ts";

const encoder = new TextEncoder();

const sum: Materializer<number, number> = {
  initial: () => 0,
  evolve: (state, event) => state + event,
};

async function sourceWith(values: number[]) {
  const protocol = createStreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
  });
  const created = await protocol.create("numbers", {
    contentType: "application/json",
  });
  if (created.status !== "created") throw new Error(`create failed: ${created.status}`);
  const offsets: string[] = [];
  for (const value of values) {
    const appended = await created.stream.append({
      contentType: "application/json",
      data: encoder.encode(JSON.stringify(value)),
    });
    if (appended.status !== "appended") throw new Error(`append failed: ${appended.status}`);
    offsets.push(appended.offset);
  }
  return { protocol, client: directProtocolClient(protocol), offsets };
}

function decodeNumbers(batch: { kind: string; items?: readonly JsonValue[] }): Iterable<number> {
  if (batch.kind !== "json") throw new Error(`expected json batch, received ${batch.kind}`);
  return batch.items as readonly number[];
}

function fold(
  client: StreamProtocolClient,
  options: { from?: string; initialState?: number } = {},
) {
  return materialize({
    source: { client, streamId: "numbers" },
    decode: decodeNumbers,
    view: sum,
    ...options,
  });
}

describe("materialize", () => {
  test("folds all currently available client batches", async () => {
    const { client, offsets } = await sourceWith([1, 2, 4, 8]);
    await expect(fold(client)).resolves.toEqual({ state: 15, cursor: offsets[3] });
  });

  test("resumes from initialState and an after-exclusive client cursor", async () => {
    const { client, offsets } = await sourceWith([1, 2, 4]);
    await expect(fold(client, { from: offsets[1], initialState: 10 })).resolves.toEqual({
      state: 14,
      cursor: offsets[2],
    });
  });

  test("resume at the tail folds no events and keeps the resume cursor", async () => {
    const { client, offsets } = await sourceWith([1, 2, 4]);
    await expect(fold(client, { from: offsets[2], initialState: 7 })).resolves.toEqual({
      state: 7,
      cursor: offsets[2],
    });
  });

  test("returns the client start cursor for an empty stream", async () => {
    const { client } = await sourceWith([]);
    const result = await fold(client);
    expect(result.state).toBe(0);
    expect(result.cursor).toBeTruthy();
  });

  test("surfaces decode errors", async () => {
    const { client } = await sourceWith([1]);
    const failure = new Error("bad event");
    await expect(
      materialize({
        source: { client, streamId: "numbers" },
        decode: () => {
          throw failure;
        },
        view: sum,
      }),
    ).rejects.toBe(failure);
  });

  test("reports missing streams through the client result contract", async () => {
    const protocol = createStreamProtocol({
      storage: { adapter: createMemoryStorageAdapter() },
    });
    await expect(fold(directProtocolClient(protocol))).rejects.toThrow(
      "Cannot materialize stream numbers: read status is not-found",
    );
  });

  test("replay is deterministic", async () => {
    const { client } = await sourceWith([1, 2, 4, 8]);
    const first = await fold(client);
    const second = await fold(client);
    expect(second).toEqual(first);
  });
});
