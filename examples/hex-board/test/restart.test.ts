// oxlint-disable-next-line effecttsgo/node-builtin-import -- This SQLite restart fixture owns native temporary files; adding platform services would require a dependency and lock change.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The native temporary-directory fixture builds the retained SQLite file path.
import { join } from "node:path";
import { it as baseIt } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Layer, Option, Result } from "effect";
import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import {
  type BoardState,
  board as hexBoard,
  GameEvents,
  initialBoard,
  stepBoard,
} from "../src/board/fold.ts";
import { moveId, MOVE_FEED_LIMIT, ProjectionIntegrityError } from "../src/board/projection.ts";
import type { GameEvent } from "../src/domain/events.ts";
import { fullGame, longGame } from "./fixtures.ts";

const board = hexBoard.at({ gameId: "game" });

const params = { gameId: "game" };

const split = fullGame.findIndex((event) => event.type === "AttackResolved");

const resolution = fullGame[split];

if (resolution?.type !== "AttackResolved") throw new Error("fixture has no AttackResolved");

const sqlite = (filename: string) =>
  CheckpointStore.layer.pipe(
    Layer.provideMerge(BunStorage.layer(Streamsy.group(GameEvents), { client: { filename } })),
  );

/**
 * Runs `program` as one process on `layer`: it opens the Layer, runs, and closes it.
 * Two calls on the same SQLite file are a restart.
 */
const asProcess =
  <ROut, LE>(fixture: Layer.Layer<ROut, LE>) =>
  <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(fixture);

        return yield* program.pipe(Effect.provideContext(services));
      }),
    );

const tempFile = (prefix: string) =>
  Effect.acquireRelease(
    Effect.sync(() => mkdtempSync(join(tmpdir(), prefix))),
    (directory) => Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  ).pipe(Effect.map((directory) => join(directory, "board.sqlite")));

/** Each half of a split game has events; an append needs at least one. */
const nonEmpty = <A>(events: ReadonlyArray<A>): Array.NonEmptyReadonlyArray<A> => {
  if (!Array.isReadonlyArrayNonEmpty(events)) throw new Error("expected events");

  return events;
};

const seed = (events: readonly GameEvent[] = fullGame, cut = split) =>
  Effect.gen(function* () {
    yield* (yield* GameEvents).create(params);
    yield* (yield* GameEvents).append(params, nonEmpty(events.slice(0, cut)));
    expect((yield* Projection.run(board)).items).toBe(cut);
  });

const snapshot = Effect.gen(function* () {
  const { state, offsets } = yield* Projection.read(board);
  const checkpoint = yield* (yield* CheckpointStore).load(board);

  return {
    state,
    offsets,
    checkpoint,
    encoded: Option.flatMap(checkpoint.record, (record) => Option.fromUndefinedOr(record.state)),
  };
});

const read = Effect.gen(function* () {
  const { state, offsets, checkpoint, encoded } = yield* snapshot;
  const tail = (yield* (yield* GameEvents).head(params)).nextOffset;
  expect(offsets.input).toBe(tail);
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
    yield* (yield* GameEvents).append(params, nonEmpty(events.slice(cut)));
    expect((yield* Projection.run(board)).items).toBe(events.length - cut);
    const after = yield* read;

    const pure = events.reduce(
      (state, event, index) =>
        Result.getOrThrow(stepBoard(state, event, index < cut ? before.tail : after.tail)),
      initialBoard({ gameId: "game" }),
    );

    expect(after.state).toEqual(pure);
    expect(after.state.ordinal).toBe(events.length);

    if (events === fullGame) expect(after.state.board.game.status).toBe("finished");
    expect(after.state.board.moves.map((move) => move.id)).toEqual(
      globalThis.Array.from({ length: Math.min(events.length, MOVE_FEED_LIMIT) }, (_, i) =>
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
  baseIt.effect(`SQLite restart retains state and checkpoint, then resumes: ${name}`, () =>
    Effect.gen(function* () {
      const file = yield* tempFile("hex-board-");
      const before = yield* seed(events, cut).pipe(Effect.andThen(read), asProcess(sqlite(file)));
      expect(before.state.ordinal).toBe(cut);

      if (events === fullGame) expect(before.state.board.combat?.status).toBe("awaiting-defense");
      else
        expect(before.state.board.moves.map((move) => move.id)).toEqual(
          globalThis.Array.from({ length: MOVE_FEED_LIMIT }, (_, i) =>
            moveId(cut - MOVE_FEED_LIMIT + i),
          ),
        );
      yield* Effect.gen(function* () {
        expect((yield* Projection.run(board)).items).toBe(0);
        expect(yield* read).toEqual(before);
        yield* advance(before, events, cut);
      }).pipe(asProcess(sqlite(file)));
    }),
  );
}

baseIt.effect(
  "a contradictory AttackResolved fails typed and leaves SQLite state and checkpoint at the prior cut",
  () =>
    Effect.gen(function* () {
      const file = yield* tempFile("hex-board-rollback-");
      yield* Effect.gen(function* () {
        yield* seed();
        const before = yield* snapshot;
        yield* (yield* GameEvents).append(params, [{ ...resolution, attackerRolls: [1, 1, 1] }]);
        const error = yield* Effect.flip(Projection.run(board));
        expect(error).toBeInstanceOf(ProjectionIntegrityError);
        expect(yield* snapshot).toEqual(before);
      }).pipe(asProcess(sqlite(file)));
    }),
);
