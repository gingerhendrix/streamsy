import { describe, expect, it } from "vitest";
import type { JsonCodec } from "@streamsy/json";
import type { DurableStateSchemaMap } from "@streamsy/state";
import { durableStateProjectionAdapter } from "./durable-state-adapter.ts";

type Event = { id: string; name: string };
type State = { rows: Event[] };
const codec = <T>(): JsonCodec<T> => ({ encode: (v) => v, decode: (v) => v as T });
const schema = {
  rows: { type: "row", primaryKey: "id", schema: codec<Event>() },
  projectionMeta: { primaryKey: () => "main", schema: codec<unknown>() },
} satisfies DurableStateSchemaMap;

describe("durableStateProjectionAdapter", () => {
  it("diffs rows and recovers the framework-owned checkpoint", () => {
    const adapter = durableStateProjectionAdapter({
      processorId: "test",
      generation: "v1",
      reducerVersion: "one",
      sourceStreamId: "source",
      outputStreamId: "output",
      sourceSchema: codec<Event>(),
      schema,
      initial: (): State => ({ rows: [] }),
      reduce: (state, event) => ({ rows: [...state.rows, event] }),
      rows: (state) => state.rows.map((value) => ({ type: "row", key: value.id, value })),
      txid: (event, transitionMeta) => `${event.id}:${transitionMeta.sourceThroughOffset}`,
      meta: { type: "projectionMeta", key: "main" },
    });
    const meta = {
      sourceStreamId: "source",
      sourceThroughOffset: "2",
      sourceSeq: 0,
      generation: "v1",
      reducerVersion: "one",
    };
    const prev: State = { rows: [] };
    const event = { id: "a", name: "Alice" };
    const next = adapter.reduce(prev, event, meta);
    const encoded = adapter.encodeTransition({ prev, next, event, meta });
    expect(encoded.map((message) => (message as { type: string }).type)).toEqual([
      "row",
      "projectionMeta",
    ]);
    expect(
      encoded.map((message) => (message as { headers: { txid?: string } }).headers.txid),
    ).toEqual(["a:2", "a:2"]);
    const bytes = encoded.map((message) => new TextEncoder().encode(JSON.stringify(message)));
    expect(adapter.decodeCheckpoint(bytes)).toEqual({
      state: next,
      sourceThroughOffset: "2",
      sourceSeq: 0,
    });
  });
});
