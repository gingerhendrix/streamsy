import { createMemoryStorageAdapter, createStreamProtocol, ZERO_OFFSET } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";
import { describe, expect, test } from "vitest";
import { materialize, type Materializer } from "./materialize.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const sum: Materializer<number, number> = {
  initial: () => 0,
  evolve: (state, event) => state + event,
};

async function sourceWith(values: number[]) {
  const protocol = createStreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
  });
  const created = await protocol.create("numbers", {
    contentType: "text/plain",
  });
  if (created.status !== "created") throw new Error(`create failed: ${created.status}`);
  const offsets: string[] = [];
  for (const value of values) {
    const appended = await created.stream.append({
      contentType: "text/plain",
      data: encoder.encode(String(value)),
    });
    if (appended.status !== "appended") throw new Error(`append failed: ${appended.status}`);
    offsets.push(appended.offset);
  }
  return { protocol, offsets };
}

function fold(
  protocol: StreamProtocolFactory,
  options: { from?: string; to?: string; initialState?: number } = {},
) {
  return materialize({
    source: { protocol, streamId: "numbers" },
    decode: (message) => Number(decoder.decode(message.data)),
    view: sum,
    ...options,
  });
}

describe("materialize", () => {
  test("folds one stream with bounded (from, to] semantics", async () => {
    const { protocol, offsets } = await sourceWith([1, 2, 4, 8]);
    const result = await fold(protocol, { from: offsets[0], to: offsets[2] });
    expect(result).toEqual({ state: 6, cursor: offsets[2] });
  });

  test("folds through the tail when to is beyond it", async () => {
    const { protocol, offsets } = await sourceWith([1, 2, 4]);
    const beyondTail = "ffffffffffffffff_ffffffffffffffff";
    await expect(fold(protocol, { from: offsets[0], to: beyondTail })).resolves.toEqual({
      state: 6,
      cursor: offsets[2],
    });
  });

  test("returns an empty fold when from equals to", async () => {
    const { protocol, offsets } = await sourceWith([1, 2]);
    await expect(fold(protocol, { from: offsets[0], to: offsets[0] })).resolves.toEqual({
      state: 0,
      cursor: offsets[0],
    });
  });

  test("returns an empty fold when from is beyond the tail", async () => {
    const { protocol } = await sourceWith([1, 2]);
    const beyondTail = "ffffffffffffffff_ffffffffffffffff";
    await expect(fold(protocol, { from: beyondTail })).resolves.toEqual({
      state: 0,
      cursor: beyondTail,
    });
  });

  test("rejects when to precedes from", async () => {
    const { protocol, offsets } = await sourceWith([1, 2]);
    await expect(fold(protocol, { from: offsets[1], to: offsets[0] })).rejects.toBeInstanceOf(
      RangeError,
    );
  });

  test("returns initial state and the starting cursor for an empty stream", async () => {
    const { protocol } = await sourceWith([]);
    await expect(fold(protocol)).resolves.toEqual({
      state: 0,
      cursor: ZERO_OFFSET,
    });
  });

  test("resumes from initialState and an after-exclusive cursor", async () => {
    const { protocol, offsets } = await sourceWith([1, 2, 4]);
    await expect(fold(protocol, { from: offsets[1], initialState: 10 })).resolves.toEqual({
      state: 14,
      cursor: offsets[2],
    });
  });

  test("resume at the tail folds no records and keeps the resume cursor", async () => {
    const { protocol, offsets } = await sourceWith([1, 2, 4]);
    await expect(fold(protocol, { from: offsets[2], initialState: 7 })).resolves.toEqual({
      state: 7,
      cursor: offsets[2],
    });
  });

  test("surfaces decode errors", async () => {
    const { protocol } = await sourceWith([1]);
    const failure = new Error("bad event");
    await expect(
      materialize({
        source: { protocol, streamId: "numbers" },
        decode: () => {
          throw failure;
        },
        view: sum,
      }),
    ).rejects.toBe(failure);
  });

  test("replay is deterministic", async () => {
    const { protocol } = await sourceWith([1, 2, 4, 8]);
    const first = await fold(protocol);
    const second = await fold(protocol);
    expect(second).toEqual(first);
  });
});
