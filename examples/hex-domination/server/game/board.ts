import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { Effect, Schema } from "effect";
import { BOARD_META_KEY, BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";
import {
  initialProjection,
  projectEvent,
  type ProjectionState,
} from "../../src/board/projection.ts";
import { ProjectionStateSchema } from "../../src/board/schemas.ts";
import { boardProjectionTxId } from "../../src/board/transaction.ts";
import { BOARD_GENERATION, eventStreamId } from "./names.ts";
import { BoardStream, GameEvents, type GameStreams } from "./streams.ts";

export const BoardState = Schema.Struct({ ordinal: Schema.Int, board: ProjectionStateSchema });
type BoardState = typeof BoardState.Type;
type BoardChange = DurableState.Change<typeof BoardStream.collections>;

/** Turn and combat are zero-or-one collections. Their removal is an explicit State delete. */
function changedRows<T extends { readonly id: string }, const K extends string>(
  type: K,
  previous: readonly T[],
  next: readonly T[],
) {
  const before = new Map(previous.map((item) => [item.id, item]));
  const changes: (DurableState.Upsert<T, K> | DurableState.Delete<never, K>)[] = [];
  for (const row of next) {
    const old = before.get(row.id);
    if (JSON.stringify(old) !== JSON.stringify(row)) changes.push(DurableState.upsert(type, row));
    before.delete(row.id);
  }
  for (const key of before.keys()) changes.push(DurableState.remove(type, key));
  return changes;
}

const singleton = <T>(row: T | null): readonly T[] => (row === null ? [] : [row]);

export function boardChanges(previous: ProjectionState, next: ProjectionState): BoardChange[] {
  return [
    ...changedRows("game", previous.sourceThroughOffset === null ? [] : [previous.game], [
      next.game,
    ]),
    ...changedRows("player", previous.players, next.players),
    ...changedRows("hex", previous.hexes, next.hexes),
    ...changedRows("territory", previous.territories, next.territories),
    ...changedRows("continent", previous.continents, next.continents),
    ...changedRows("turn", singleton(previous.turn), singleton(next.turn)),
    ...changedRows("combat", singleton(previous.combat), singleton(next.combat)),
    ...changedRows("move", previous.moves, next.moves),
  ];
}

export const boardProjection = Projection.mapAccum({
  id: "hex-domination-board",
  generation: 2,
  commit: Commit.producer,
  // Keep the archived text generation ids (including explicit rebuild names).
  // The output route and checkpoint key both bind the generation parameter.
  params: BoardStream.paramCodecs,
  inputs: { events: GameEvents },
  outputs: { board: BoardStream },
  state: {
    schema: BoardState,
    initial: ({ gameId }): BoardState => ({ ordinal: 0, board: initialProjection(gameId) }),
  },
  mapAccum: (state, { item, offset, index }, { params: { gameId, generation }, ranges }) =>
    Effect.sync(() => {
      if (offset === undefined) throw new Error("Board projection requires local command offsets");
      if (index === 0 && (state.board.sourceThroughOffset ?? ZERO_OFFSET) !== ranges.events?.from) {
        throw new Error("Board checkpoint disagrees with its canonical watermark");
      }
      let board = state.board;
      let ordinal = state.ordinal;
      for (const event of item.events) board = projectEvent(board, event, offset, ordinal++);
      const changes = boardChanges(state.board, board);
      // The receipt is the last fact for this command. Earlier facts do not
      // advertise its txid, so even a paginated browser read cannot confirm early.
      changes.push(
        DurableState.upsert(
          "projectionMeta",
          {
            id: BOARD_META_KEY,
            sourceStreamId: eventStreamId(gameId),
            sourceThroughOffset: offset,
            sourceSeq: ordinal - 1,
            generation,
            reducerVersion: BOARD_REDUCER_VERSION,
            snapshot: board,
          },
          { headers: { txid: boardProjectionTxId(item.events[0].commandId) } },
        ),
      );
      return [{ ordinal, board }, { board: changes }] as const;
    }),
});

export interface MaterializedBoard {
  state: ProjectionState;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
}

export function materializeBoard(
  streams: GameStreams,
  gameId: string,
  generation: string = BOARD_GENERATION,
): Promise<MaterializedBoard> {
  return streams.runPromise(
    Effect.gen(function* () {
      const binding = boardProjection.at({ gameId, generation });
      const result = yield* Projection.run(binding);
      if (result.status === "limit-reached")
        return yield* Effect.die(new Error("Board projection did not catch up"));
      const through = result.offsets.events || null;
      if (result.state.board.sourceThroughOffset !== through) {
        return yield* Effect.die(
          new Error("Board checkpoint disagrees with its canonical watermark"),
        );
      }
      return {
        state: result.state.board,
        sourceStreamId: eventStreamId(gameId),
        sourceThroughOffset: through,
        generation,
      };
    }),
  );
}
