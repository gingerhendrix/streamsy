import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Effect, Layer, ManagedRuntime, Option } from "effect";
import { Streams } from "@streamsy/core";
import { Checkpoints, Projection, State } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { BoardState, boardProjection, initialBoard, stepBoard } from "../src/board/fold.ts";
import { fullGame } from "./fixtures.ts";

const board = boardProjection("game");
const input = board.inputs.input;
const split = fullGame.findIndex((event) => event.type === "AttackResolved");
const sqlite = (filename: string) =>
  Sqlite.layer.pipe(Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })));
const seed = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.append(input, fullGame.slice(0, split));
  expect((yield* Projection.run(board)).items).toBe(split);
});
const read = Effect.gen(function* () {
  const state = Option.getOrThrow(yield* Projection.loadState(board, BoardState));
  const checkpoint = yield* (yield* Checkpoints).load(board);
  const tail = (yield* Streams.head(input)).nextOffset;
  expect(Option.getOrThrow(checkpoint.record).inputs.input).toBe(tail);
  expect(state.board.sourceThroughOffset).toBe(tail);
  return { state, checkpoint, tail, encoded: yield* (yield* State).load(board) };
});
const advance = (before: { state: BoardState; tail: string }) =>
  Effect.gen(function* () {
    yield* Streams.append(input, fullGame.slice(split));
    expect((yield* Projection.run(board)).items).toBe(fullGame.length - split);
    const after = yield* read;
    const pure = fullGame.reduce(
      (state, event, index) => stepBoard(state, event, index < split ? before.tail : after.tail),
      initialBoard("game"),
    );
    expect(after.state).toEqual(pure);
    expect(after.state.ordinal).toBe(fullGame.length);
    expect(after.state.board.game.status).toBe("finished");
    expect(new Set(after.state.board.moves.map((move) => move.id)).size).toBe(
      after.state.board.moves.length,
    );
    expect((yield* Projection.run(board)).items).toBe(0);
  });

test("SQLite restart retains the board, ordinal and tail checkpoint, then resumes", async () => {
  const directory = mkdtempSync("/tmp/hex-board-");
  const filename = join(directory, "board.sqlite");
  try {
    const first = ManagedRuntime.make(sqlite(filename));
    const before = await first
      .runPromise(seed.pipe(Effect.andThen(read)))
      .finally(() => first.dispose());
    expect(before.state.board.combat?.status).toBe("awaiting-defense");
    const second = ManagedRuntime.make(sqlite(filename));
    try {
      expect((await second.runPromise(Projection.run(board))).items).toBe(0);
      expect(await second.runPromise(read)).toEqual(before);
      await second.runPromise(advance(before));
    } finally {
      await second.dispose();
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("memory folds the same game incrementally", async () => {
  const runtime = ManagedRuntime.make(Memory.layerMemory());
  try {
    const before = await runtime.runPromise(seed.pipe(Effect.andThen(read)));
    await runtime.runPromise(advance(before));
  } finally {
    await runtime.dispose();
  }
});
