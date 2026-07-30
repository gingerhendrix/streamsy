import { describe, expect, it } from "vitest";

import {
  ACTIONS_STREAM_TIMEOUT_MS,
  actionsControlFrame,
  actionsDataFrame,
  createActionsDecoder,
  createSseParser,
} from "./actions-stream.ts";

/** Feed a whole document one byte at a time — the worst chunking a client sees. */
function pushByBytes<T>(decoder: { push(chunk: string): T[] }, document: string): T[] {
  return [...document].flatMap((character) => decoder.push(character));
}

describe("actions stream framing", () => {
  it("holds one bound for every client to size itself against", () => {
    expect(ACTIONS_STREAM_TIMEOUT_MS).toBe(30_000);
  });

  it("round-trips a batch regardless of where chunk boundaries fall", () => {
    const messages = [
      { type: "ActionRequired", seq: 1, note: "a value with\nan embedded newline" },
      { type: "ActionRequired", seq: 2 },
    ];
    const document =
      actionsDataFrame(messages) + actionsControlFrame({ nextOffset: "off-2", upToDate: true });

    const whole = createActionsDecoder().push(document);
    expect(whole).toEqual([{ messages, nextOffset: "off-2", upToDate: true, closed: false }]);
    // Byte-at-a-time delivery yields exactly the same single batch.
    expect(pushByBytes(createActionsDecoder(), document)).toEqual(whole);
  });

  it("emits a batch only at its control event", () => {
    const decoder = createActionsDecoder();
    // Messages alone are not a batch: a cursor may not advance past them yet.
    expect(decoder.push(actionsDataFrame([{ seq: 1 }]))).toEqual([]);
    expect(decoder.push(actionsControlFrame({ nextOffset: "off-1", upToDate: true }))).toEqual([
      { messages: [{ seq: 1 }], nextOffset: "off-1", upToDate: true, closed: false },
    ]);
    // An empty hold still re-states the cursor.
    expect(decoder.push(actionsControlFrame({ nextOffset: "off-1", upToDate: true }))).toEqual([
      { messages: [], nextOffset: "off-1", upToDate: true, closed: false },
    ]);
  });

  it("reports the terminal batch as closed", () => {
    const decoder = createActionsDecoder();
    const batches = decoder.push(
      actionsDataFrame([{ type: "GameOver" }]) +
        actionsControlFrame({ nextOffset: "off-9", upToDate: true, closed: true }),
    );
    expect(batches).toEqual([
      { messages: [{ type: "GameOver" }], nextOffset: "off-9", upToDate: true, closed: true },
    ]);
  });

  it("parses multi-line data, CRLF line endings, and comment heartbeats", () => {
    const frames = createSseParser().push(
      ':keep-alive\r\nevent: data\r\ndata:[\r\ndata:{"seq":1}\r\ndata:]\r\n\r\n',
    );
    expect(frames).toEqual([{ event: "data", data: '[\n{"seq":1}\n]' }]);
    expect(JSON.parse(frames[0]!.data)).toEqual([{ seq: 1 }]);
  });
});
