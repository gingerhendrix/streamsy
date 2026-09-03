/* oxlint-disable effecttsgo/async-function -- This Bun SQLite integration suite has one Promise-returning runner and adapter execution model. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun SQLite adapter creates its isolated temporary database directory through Node fs.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun SQLite adapter constructs its temporary database filename through Node path.
import { join as joinPath } from "node:path";
import { describe, expect, test } from "bun:test";
import { StreamProtocol, directProtocolClient } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { Effect, Layer, Option } from "effect";
import { bindStream } from "@streamsy/streams/binding";
import { streamIdentity } from "@streamsy/streams/identity";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/streams";
import { provideTestLayers } from "@streamsy/streams/testing";
import { DerivedRecoveryLive, DerivedStateHistoryLive } from "./derived-append.ts";
import { catchUpDynamicFanInState, FanInRecoveryLive } from "./fan-in-state.ts";
import { deriveProducerLane } from "./lane.ts";
import { catchUpState } from "./state-projection.ts";
import {
  decodeBoardFactOption,
  decodeMemberValue,
  decodeMembershipFact,
  decodeTotalFactOption,
} from "./state-test-fixtures.ts";

const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };
const StateProjectionTestLive = Layer.merge(DerivedRecoveryLive, DerivedStateHistoryLive).pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);
const FanInTestLive = FanInRecoveryLive.pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);

describe("recovered State and dynamic fan-in — SQLite", () => {
  test("restores application state and member cursors after reopening the database", async () => {
    const filename = joinPath(mkdtempSync(joinPath(tmpdir(), "streamsy-fan-in-")), "state.sqlite");

    const first = await makeHarness(filename);
    await first.client.stream("events").create({ contentType: "application/json" });
    await first.client.stream("detail").create({ contentType: "application/json" });
    await first.client.stream("membership").create({ contentType: "application/json" });
    await first.client.stream("board").create({ contentType: "application/json" });
    await first.client.stream("events").appendJsonBatch([{ v: 3 }]);
    await first.client.stream("membership").appendJsonBatch([{ type: "join", member: "detail" }]);

    expect(await first.detail()).toMatchObject({ status: "caught-up", state: { total: 3 } });
    expect(await first.board()).toMatchObject({ status: "caught-up", state: { detail: 3 } });
    await first.close();

    const reopened = await makeHarness(filename);
    await reopened.client.stream("events").appendJsonBatch([{ v: 4 }]);
    const detail = await reopened.detail();
    expect(detail).toMatchObject({ status: "caught-up", batches: 1, state: { total: 7 } });
    const board = await reopened.board();
    expect(board).toMatchObject({ status: "caught-up", batches: 1, state: { detail: 7 } });
    await reopened.close();
  });
});

async function makeHarness(filename: string) {
  const adapter = createSqliteStorageAdapter({ filename });
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const eventsIdentity = streamIdentity("events");
  const detailIdentity = streamIdentity("detail");
  const membershipIdentity = streamIdentity("membership");
  const boardIdentity = streamIdentity("board");
  const events = bindStream({ identity: eventsIdentity, client, streamId: "events" });
  const detailBinding = bindStream({ identity: detailIdentity, client, streamId: "detail" });
  const membership = bindStream({ identity: membershipIdentity, client, streamId: "membership" });
  const board = bindStream({ identity: boardIdentity, client, streamId: "board" });
  const detailLane = await deriveProducerLane({
    processorId: "detail",
    processorVersion: "1",
    outputGeneration: "1",
    source: eventsIdentity,
    target: detailIdentity,
    producerEpoch: 1,
  });
  const boardLane = await deriveProducerLane({
    processorId: "board",
    processorVersion: "1",
    outputGeneration: "1",
    source: membershipIdentity,
    target: boardIdentity,
    producerEpoch: 1,
  });

  return {
    adapter,
    client,
    detail: () =>
      Effect.runPromise(
        catchUpState<{ total: number }, number>({
          source: events,
          target: detailBinding,
          lane: detailLane,
          limits,
          initial: { total: 0 },
          restore: (initial, facts) =>
            facts.reduce<{ total: number }>((state, encodedFact) => {
              const fact = decodeTotalFactOption(encodedFact);
              return Option.isSome(fact) ? { total: fact.value.value.total } : state;
            }, initial),
          validateRecovered: () => {},
          decode: (batch) => {
            if (batch.kind !== "json") throw new Error("expected JSON");
            return batch.items.map((item) => decodeMemberValue(item).v);
          },
          step: (state, values) => {
            const total = state.total + values.reduce((sum, value) => sum + value, 0);
            return {
              facts: [
                { type: "total", key: "total", value: { total }, headers: { operation: "upsert" } },
              ],
            };
          },
        }).pipe((effect) => provideTestLayers(effect, StateProjectionTestLive)),
      ),
    board: () =>
      Effect.runPromise(
        catchUpDynamicFanInState<Record<string, number>, number>({
          membership,
          target: board,
          lane: boardLane,
          limits,
          initial: {},
          restore: (initial, facts) =>
            facts.reduce<Record<string, number>>((state, encodedFact) => {
              const decoded = decodeBoardFactOption(encodedFact);
              if (Option.isNone(decoded)) return state;
              const fact = decoded.value;
              if (!("value" in fact)) {
                const { [fact.key]: _removed, ...rest } = state;
                return rest;
              }
              return { ...state, [fact.key]: fact.value.last };
            }, initial),
          decodeMembership: (batch) => {
            if (batch.kind !== "json") throw new Error("expected JSON membership");
            return batch.items.map((encodedItem) => {
              const item = decodeMembershipFact(encodedItem);
              return item.type === "leave"
                ? ({ type: "leave", member: streamIdentity(item.member) } as const)
                : ({ type: "join", member: streamIdentity(item.member) } as const);
            });
          },
          resolveMember: (identity) => (identity.name === "detail" ? detailBinding : undefined),
          decodeMember: (batch) => {
            if (batch.kind !== "json") throw new Error("expected JSON member");
            return batch.items.flatMap((item) => {
              const fact = decodeTotalFactOption(item);
              return Option.isSome(fact) ? [fact.value.value.total] : [];
            });
          },
          onRecord: (state, member, values) => {
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
          onRemove: (state, member) => {
            const { [member.identity.name]: _removed, ...rest } = state;
            return {
              state: rest,
              facts: [{ type: "row", key: member.identity.name, headers: { operation: "delete" } }],
            };
          },
        }).pipe((effect) => provideTestLayers(effect, FanInTestLive)),
      ),
    async close() {
      await client.close();
      adapter.close();
    },
  };
}
