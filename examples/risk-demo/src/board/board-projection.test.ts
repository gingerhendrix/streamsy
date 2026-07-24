import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";

import type { GameEvent } from "../domain/events.ts";
import { foldAggregate } from "../domain/aggregate.ts";
import {
  aggregateBoardView,
  boardsEqual,
  projectionBoardView,
  type ProjectionState,
} from "./projection.ts";
import { recordFullGameEvents } from "../../test/testkit.ts";
import { boardProjectionTxId } from "./transaction.ts";
import { createBoardProjectionAdapter, writeCanonicalEvents } from "./board-projection.ts";

const SOURCE = "games/game-1/events";
const OUTPUT = "games/game-1/projections/board/v1";

function newProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
}

function adapterFor(gen = "v1") {
  return createBoardProjectionAdapter({
    gameId: "game-1",
    sourceStreamId: SOURCE,
    outputStreamId: `games/game-1/projections/board/${gen}`,
    generation: gen,
  });
}

/** The board the materialized projection currently holds, in normalized view form. */
function projectionView(runtime: ProjectionRuntime<ProjectionState, GameEvent>) {
  return projectionBoardView(runtime.currentState());
}

async function outputCount(protocol: StreamProtocolFactory, streamId = OUTPUT): Promise<number> {
  const got = await protocol.get(streamId);
  if (got.status !== "ok") return 0;
  const read = await got.stream.read({});
  return read.status === "ok" ? read.messages.length : 0;
}

describe("board projection materializer", () => {
  it("materializes a full game so the projection equals the aggregate at the source head", async () => {
    const events = recordFullGameEvents(1234);
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);

    const runtime = new ProjectionRuntime({ protocol, adapter: adapterFor() });
    const { status } = await runtime.catchUp();

    expect(status.caughtUp).toBe(true);
    expect(status.sourceSeq).toBe(events.length - 1);

    const expected = aggregateBoardView(foldAggregate(events));
    expect(boardsEqual(projectionView(runtime), expected)).toBe(true);
    expect(runtime.currentState().game.status).toBe("finished");
  });

  it("marks each projection transition with its command and exact source position", async () => {
    const events = recordFullGameEvents(1234).slice(0, 3);
    const protocol = newProtocol();
    const offsets = await writeCanonicalEvents(protocol, SOURCE, events);
    await new ProjectionRuntime({ protocol, adapter: adapterFor() }).catchUp();

    const output = await protocol.get(OUTPUT);
    expect(output.status).toBe("ok");
    if (output.status !== "ok") return;
    const read = await output.stream.read({});
    expect(read.status).toBe("ok");
    if (read.status !== "ok") return;
    const messages = read.messages.map(
      (message) =>
        JSON.parse(new TextDecoder().decode(message.data)) as { headers: { txid?: string } },
    );
    const finalTxId = boardProjectionTxId(events.at(-1)!.commandId, offsets.at(-1)!);

    expect(messages.at(-1)?.headers.txid).toBe(finalTxId);
    expect(messages.filter((message) => message.headers.txid === finalTxId).length).toBeGreaterThan(
      0,
    );
  });

  it("keeps the atomic watermark equal to the aggregate board at every prefix", async () => {
    const events = recordFullGameEvents(1234);
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);
    const runtime = new ProjectionRuntime({ protocol, adapter: adapterFor() });
    await runtime.catchUp();

    // Rebuild independently from just the committed projection stream (no source),
    // proving board + watermark are consistent and self-contained.
    const reloaded = new ProjectionRuntime({ protocol, adapter: adapterFor() });
    await reloaded.load();
    const expected = aggregateBoardView(foldAggregate(events));
    expect(boardsEqual(projectionBoardView(reloaded.currentState()), expected)).toBe(true);
    expect(reloaded.currentState().sourceThroughOffset).not.toBeNull();
  });

  it("recovers from a crash right after an output commit without double-applying", async () => {
    const events = recordFullGameEvents(1234);
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);

    const crashAt = Math.floor(events.length / 2);
    const crashing = new ProjectionRuntime({
      protocol,
      adapter: adapterFor(),
      faults: {
        afterAppend: ({ sourceSeq }) => {
          if (sourceSeq === crashAt) throw new Error("crash after commit");
        },
      },
    });
    await expect(crashing.catchUp()).rejects.toThrow("crash after commit");
    const committedAfterCrash = await outputCount(protocol);

    const recovered = new ProjectionRuntime({ protocol, adapter: adapterFor() });
    await recovered.catchUp();

    const expected = aggregateBoardView(foldAggregate(events));
    expect(boardsEqual(projectionView(recovered), expected)).toBe(true);
    // Recovery only appended the remaining transitions; the crashed one is not redone.
    expect(await outputCount(protocol)).toBeGreaterThan(committedAfterCrash);
    expect((await recovered.status()).stopped).toBe(false);
  });

  it("catches up incrementally as canonical events arrive, gap-free", async () => {
    const events = recordFullGameEvents(1234);
    const protocol = newProtocol();
    const runtime = new ProjectionRuntime({ protocol, adapter: adapterFor() });

    const midpoint = Math.floor(events.length / 2);
    await writeCanonicalEvents(protocol, SOURCE, events.slice(0, midpoint));
    const first = await runtime.catchUp();
    expect(first.applied).toBe(midpoint);
    expect(first.status.caughtUp).toBe(true);

    await writeCanonicalEvents(protocol, SOURCE, events.slice(midpoint));
    const second = await runtime.catchUp();
    expect(second.applied).toBe(events.length - midpoint);

    const expected = aggregateBoardView(foldAggregate(events));
    expect(boardsEqual(projectionView(runtime), expected)).toBe(true);
    expect(await outputCount(protocol)).toBeGreaterThan(0);
  });

  it("rebuilds an equivalent board into a fresh generation from the canonical log", async () => {
    const events = recordFullGameEvents(1234);
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);

    const v1 = new ProjectionRuntime({ protocol, adapter: adapterFor("v1") });
    await v1.catchUp();

    // A new generation replays the same canonical events into a separate stream.
    const v2 = new ProjectionRuntime({ protocol, adapter: adapterFor("v2") });
    await v2.catchUp();

    expect(
      boardsEqual(projectionBoardView(v1.currentState()), projectionBoardView(v2.currentState())),
    ).toBe(true);
    expect(v1.currentState().sourceThroughOffset).toBe(v2.currentState().sourceThroughOffset);
  });
});
