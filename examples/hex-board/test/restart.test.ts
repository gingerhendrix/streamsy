import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Effect, Layer, ManagedRuntime, Option, Exit, Cause } from "effect";
import { Streams } from "@streamsy/core";
import { Checkpoints, Projection, State } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { BoardState, boardProjection, initialBoard, stepBoard } from "../src/board/fold.ts";
import { moveId, MOVE_FEED_LIMIT } from "../src/board/projection.ts";
import type { GameEvent } from "../src/domain/events.ts";
import { fullGame, longGame } from "./fixtures.ts";

const board = boardProjection("game");
const input = board.inputs.input;
const split = fullGame.findIndex((event) => event.type === "AttackResolved");
const sqlite = (filename: string) =>
  Sqlite.layer.pipe(Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })));
const seed = (events: readonly GameEvent[] = fullGame, cut = split) =>
  Effect.gen(function* () {
    yield* Streams.create(input);
    yield* Streams.append(input, events.slice(0, cut));
    expect((yield* Projection.run(board)).items).toBe(cut);
  });
const snapshot = Effect.gen(function* () {
  const state = Option.getOrThrow(yield* Projection.loadState(board, BoardState));
  const checkpoint = yield* (yield* Checkpoints).load(board);
  return { state, checkpoint, encoded: yield* (yield* State).load(board) };
});
const read = Effect.gen(function* () {
  const { state, checkpoint, encoded } = yield* snapshot;
  const tail = (yield* Streams.head(input)).nextOffset;
  expect(Option.getOrThrow(checkpoint.record).inputs.input).toBe(tail);
  expect(state.board.sourceThroughOffset).toBe(tail);
  return { state, checkpoint, tail, encoded };
});
const advance = (
  before: { state: BoardState; tail: string },
  events: readonly GameEvent[] = fullGame,
  cut = split,
) =>
  Effect.gen(function* () {
    yield* Streams.append(input, events.slice(cut));
    expect((yield* Projection.run(board)).items).toBe(events.length - cut);
    const after = yield* read;
    const pure = events.reduce(
      (state, event, index) => stepBoard(state, event, index < cut ? before.tail : after.tail),
      initialBoard("game"),
    );
    expect(after.state).toEqual(pure);
    expect(after.state.ordinal).toBe(events.length);
    if (events === fullGame) expect(after.state.board.game.status).toBe("finished");
    expect(after.state.board.moves.map((move) => move.id)).toEqual(
      Array.from({ length: Math.min(events.length, MOVE_FEED_LIMIT) }, (_, i) =>
        moveId(Math.max(0, events.length - MOVE_FEED_LIMIT) + i),
      ),
    );
    expect(new Set(after.state.board.moves.map((move) => move.id)).size).toBe(
      after.state.board.moves.length,
    );
    expect((yield* Projection.run(board)).items).toBe(0);
  });

for (const [name, events, cut] of [
  ["12-event victory game split after 5 events", fullGame, split],
  ["450-event game split after 400 events", longGame, 400],
] as const) {
  test(`SQLite restart retains state and checkpoint, then resumes: ${name}`, async () => {
    const directory = mkdtempSync("/tmp/hex-board-");
    const filename = join(directory, "board.sqlite");
    try {
      const first = ManagedRuntime.make(sqlite(filename));
      const before = await first
        .runPromise(seed(events, cut).pipe(Effect.andThen(read)))
        .finally(() => first.dispose());
      expect(before.state.ordinal).toBe(cut);
      if (events === fullGame) expect(before.state.board.combat?.status).toBe("awaiting-defense");
      else
        expect(before.state.board.moves.map((move) => move.id)).toEqual(
          Array.from({ length: MOVE_FEED_LIMIT }, (_, i) => moveId(cut - MOVE_FEED_LIMIT + i)),
        );
      const second = ManagedRuntime.make(sqlite(filename));
      try {
        expect((await second.runPromise(Projection.run(board))).items).toBe(0);
        expect(await second.runPromise(read)).toEqual(before);
        await second.runPromise(advance(before, events, cut));
      } finally {
        await second.dispose();
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
}

test("memory folds the same game incrementally", async () => {
  const runtime = ManagedRuntime.make(Memory.layerMemory());
  try {
    const before = await runtime.runPromise(seed().pipe(Effect.andThen(read)));
    await runtime.runPromise(advance(before));
  } finally {
    await runtime.dispose();
  }
});

test("a contradictory AttackResolved leaves SQLite state and checkpoint at the prior cut", async () => {
  const directory = mkdtempSync("/tmp/hex-board-rollback-");
  const runtime = ManagedRuntime.make(sqlite(join(directory, "board.sqlite")));
  try {
    await runtime.runPromise(seed());
    const before = await runtime.runPromise(snapshot);
    const event = fullGame[split];
    if (event?.type !== "AttackResolved") throw new Error("fixture has no resolution at split");
    await runtime.runPromise(Streams.append(input, [{ ...event, attackerRolls: [1, 1, 1] }]));
    const result = await runtime.runPromiseExit(Projection.run(board));
    expect(Exit.isFailure(result)).toBe(true);
    if (Exit.isFailure(result))
      expect(Cause.pretty(result.cause)).toContain("ProjectionIntegrityError");
    expect(await runtime.runPromise(snapshot)).toEqual(before);
  } finally {
    await runtime.dispose();
    rmSync(directory, { recursive: true, force: true });
  }
});
