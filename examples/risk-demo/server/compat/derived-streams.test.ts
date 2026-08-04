/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
import { describe, expect, it } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  type AppendOptions,
  type ProtocolStream,
  type StreamProtocolFactory,
} from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";
import { Schema } from "effect";

import { catchUpDerived } from "./derived-streams.ts";

interface Value {
  kind: string;
}

const codec: JsonCodec<Value> = {
  encode: (value) => value,
  decode: Schema.decodeUnknownSync(Schema.Struct({ kind: Schema.String })),
};

function raceFirstOutputAppend(
  base: StreamProtocolFactory,
  outputStreamId: string,
  winningPayload: Value,
): StreamProtocolFactory {
  const encoder = new TextEncoder();
  let raced = false;

  function wrap(stream: ProtocolStream): ProtocolStream {
    return {
      id: stream.id,
      async append(options: AppendOptions) {
        if (stream.id === outputStreamId && options.producer?.producerSeq === 0 && !raced) {
          raced = true;
          const winner = await stream.append({
            ...options,
            data: encoder.encode(JSON.stringify(winningPayload)),
          });
          if (winner.status !== "appended") throw new Error(`race winner: ${winner.status}`);
        }
        return stream.append(options);
      },
      read: (options) => stream.read(options),
      readNext: (options) => stream.readNext(options),
      metadata: () => stream.metadata(),
      delete: () => stream.delete(),
    };
  }

  return {
    offsetGenerator: base.offsetGenerator,
    isValidOffset: (offset) => base.isValidOffset(offset),
    onAfterCommit: (hook) => base.onAfterCommit(hook),
    async create(streamId, options) {
      const result = await base.create(streamId, options);
      if (result.status === "created" || result.status === "exists") {
        return { ...result, stream: wrap(result.stream) };
      }
      return result;
    },
    async get(streamId) {
      const result = await base.get(streamId);
      return result.status === "ok" ? { ...result, stream: wrap(result.stream) } : result;
    },
  };
}

describe("private-action durable reconciliation", () => {
  it("fails when a different payload wins the producer tuple before the response is observed", async () => {
    const base = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const source = await createJsonProtocol(base, codec).getOrCreate("events", {
      initialMessage: { kind: "source" },
    });
    expect((await source.readAll()).values).toEqual([{ kind: "source" }]);

    const protocol = raceFirstOutputAppend(base, "actions/player-1", { kind: "wrong-winner" });
    await expect(
      catchUpDerived({
        protocol,
        sourceStreamId: "events",
        sourceSchema: codec,
        outputSchema: codec,
        derive: () => new Map([["player-1", [{ kind: "expected" }]]]),
        streamIdFor: (playerId) => `actions/${playerId}`,
        producerIdFor: (playerId) => `actions:${playerId}`,
      }),
    ).rejects.toThrow(
      "derived stream actions/player-1 diverged at producer sequence 0: " +
        "a different durable payload won",
    );

    const durable = await createJsonProtocol(base, codec).getOrCreate("actions/player-1");
    expect((await durable.readAll()).values).toEqual([{ kind: "wrong-winner" }]);
  });
});
