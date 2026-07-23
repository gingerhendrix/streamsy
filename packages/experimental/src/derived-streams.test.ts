import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";
import { catchUpDerived, readDerived } from "./derived-streams.ts";

type Source = { key: string; value: number };
type Output = { value: number };
const sourceSchema: JsonCodec<Source> = { encode: (v) => v, decode: (v) => v as Source };
const outputSchema: JsonCodec<Output> = { encode: (v) => v, decode: (v) => v as Output };

describe("derived streams", () => {
  it("replays idempotently and resumes reads from a cursor", async () => {
    const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const source = await createJsonProtocol(protocol, sourceSchema).getOrCreate("source");
    await source.append({ key: "a", value: 1 });
    await source.append({ key: "a", value: 2 });
    const options = {
      protocol,
      sourceStreamId: "source",
      sourceSchema,
      outputSchema,
      derive: (messages: readonly { value: Source }[]) =>
        new Map([["a", messages.map((message) => ({ value: message.value.value }))]]),
      streamIdFor: (key: string) => `derived/${key}`,
      producerIdFor: (key: string) => `producer/${key}`,
    };
    await catchUpDerived(options);
    await catchUpDerived(options);

    const first = await readDerived(protocol, "derived/a", outputSchema);
    expect(first.values).toEqual([{ value: 1 }, { value: 2 }]);
    await source.append({ key: "a", value: 3 });
    await catchUpDerived(options);
    const resumed = await readDerived(protocol, "derived/a", outputSchema, {
      cursor: first.cursor,
    });
    expect(resumed.values).toEqual([{ value: 3 }]);
  });
});
