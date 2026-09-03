/* oxlint-disable effecttsgo/async-function -- This Bun SQLite integration suite has one Promise-returning runner and adapter execution model. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun SQLite adapter creates its isolated temporary database directory through Node fs.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun SQLite adapter constructs its temporary database filename through Node path.
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { StreamProtocol, directProtocolClient } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { Effect, Exit, Layer } from "effect";
import { bindStream, type StreamBinding } from "@streamsy/streams/binding";
import { streamIdentity } from "@streamsy/streams/identity";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/streams";
import { provideTestLayers } from "@streamsy/streams/testing";
import { DerivedRecoveryLive, DerivedStateHistoryLive } from "./derived-append.ts";
import { deriveProducerLane } from "./lane.ts";
import { catchUpState } from "./state-projection.ts";
import {
  decodeMemberValue,
  decodeTotalFact,
  lostResponseClient,
  replaceFirstTotal,
} from "./state-test-fixtures.ts";

interface Total {
  readonly total: number;
}

const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };
const StateProjectionTestLive = Layer.merge(DerivedRecoveryLive, DerivedStateHistoryLive).pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);

describe("Effect-first recovered State — SQLite", () => {
  test("validates restored application state before decoding resumed source", async () => {
    const filename = databasePath();
    const first = await makeHarness(filename);
    await first.client.stream("source").create({ contentType: "application/json" });
    await first.client.stream("target").create({ contentType: "application/json" });
    await first.client.stream("source").appendJsonBatch([{ v: 2 }]);
    await first.run();
    await first.close();

    const restarted = await makeHarness(filename);
    await restarted.client.stream("source").appendJsonBatch([{ v: 3 }]);
    let decoded = 0;
    const exit = await Effect.runPromiseExit(
      restarted.program({
        onDecode: () => decoded++,
        validateRecovered: () => {
          throw new Error("persisted state rejected");
        },
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(decoded).toBe(0);
    await restarted.close();
  });

  test("a proposed fact rejected by the fold leaves the SQLite target empty", async () => {
    const h = await makeHarness(databasePath());
    await h.client.stream("source").create({ contentType: "application/json" });
    await h.client.stream("target").create({ contentType: "application/json" });
    await h.client.stream("source").appendJsonBatch([{ v: 9 }]);
    const exit = await Effect.runPromiseExit(h.program({ rejectTotal: 9 }));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(await h.adapter.listMessages("target")).toHaveLength(0);
    await h.close();
  });

  test("a lost-response duplicate validates the durable SQLite checkpoint", async () => {
    const h = await makeHarness(databasePath());
    await h.client.stream("source").create({ contentType: "application/json" });
    await h.client.stream("target").create({ contentType: "application/json" });
    await h.client.stream("source").appendJsonBatch([{ v: 1 }]);
    const underlying = h.target.client;
    const racingClient = lostResponseClient(underlying, (items) => replaceFirstTotal(items, 999));
    const target = { ...h.target, client: racingClient };
    const exit = await Effect.runPromiseExit(
      h.program({
        target,
        validateRecovered: async ({ state }) => {
          await Promise.resolve();
          if (state.total === 999) throw new Error("durable duplicate rejected");
        },
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(await h.adapter.listMessages("target")).toHaveLength(2);
    await h.close();
  });
});

function databasePath(): string {
  return join(mkdtempSync(join(tmpdir(), "streamsy-state-laws-")), "state.sqlite");
}

async function makeHarness(filename: string) {
  const adapter = createSqliteStorageAdapter({ filename });
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const sourceIdentity = streamIdentity("source");
  const targetIdentity = streamIdentity("target");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "source" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "target" });
  const lane = await deriveProducerLane({
    processorId: "sqlite-state",
    processorVersion: "1",
    outputGeneration: "1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 1,
  });
  const program = (
    options: {
      readonly target?: StreamBinding;
      readonly rejectTotal?: number;
      readonly onDecode?: () => void;
      readonly validateRecovered?: (recovered: { readonly state: Total }) => void | Promise<void>;
    } = {},
  ) =>
    catchUpState<Total, number>({
      source,
      target: options.target ?? target,
      lane,
      limits,
      initial: { total: 0 },
      restore(initial, facts) {
        return facts.reduce((_state, encodedFact) => {
          const fact = decodeTotalFact(encodedFact);
          if (fact.value.total === options.rejectTotal) throw new Error("rejected total");
          return { total: fact.value.total };
        }, initial);
      },
      validateRecovered: options.validateRecovered ?? (() => {}),
      decode(batch) {
        options.onDecode?.();
        if (batch.kind !== "json") throw new Error("expected JSON");
        return batch.items.map((item) => decodeMemberValue(item).v);
      },
      step(state, values) {
        const total = state.total + values.reduce((sum, value) => sum + value, 0);
        return {
          facts: [
            { type: "total", key: "total", value: { total }, headers: { operation: "upsert" } },
          ],
        };
      },
    }).pipe((effect) => provideTestLayers(effect, StateProjectionTestLive));
  return {
    adapter,
    client,
    target,
    program,
    run: () => Effect.runPromise(program()),
    async close() {
      await client.close();
      adapter.close();
    },
  };
}
