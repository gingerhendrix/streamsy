/* oxlint-disable effecttsgo/async-function, effecttsgo/global-date -- The offline Bun CLI coordinates the storage runtime and the synchronous metadata transaction. */
import { CreateResult, DurableState, StreamAddress } from "@streamsy/core";
import { Effect } from "effect";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import {
  aggregateBoardView,
  boardsEqual,
  initialProjection,
  projectionBoardView,
} from "../../src/board/projection.ts";
import { BOARD_META_KEY, BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";
import { canonicalJson } from "../../src/domain/hex-generator.ts";
import { boardChanges, materializeBoard } from "./board.ts";
import { readCanonical } from "./command-service.ts";
import { boardStreamId, eventStreamId, nextGeneration } from "./names.ts";
import type { Stores } from "../persistence/stores.ts";
import { BoardStream, type GameStreams } from "./streams.ts";

export interface RebuildDeps {
  streams: GameStreams;
  stores: Stores;
}

export interface RebuildOptions {
  generation?: string;
  now?: () => number;
  /** Test seam for a failed or corrupt candidate build. Production uses the live board projection. */
  materialize?: typeof materializeBoard;
}

export interface RebuildEquivalence {
  /** The rebuilt board equals the authoritative aggregate fold. */
  boardEqual: boolean;
  /** The rebuilt watermark equals the canonical source head. */
  watermarkEqual: boolean;
}

export interface RebuildResult {
  status: "cutover" | "verification-failed" | "not-found";
  gameId: string;
  fromGeneration: string;
  toGeneration: string;
  /** Canonical source head the rebuild caught up to (null for an empty log). */
  canonicalHead: string | null;
  /** The rebuilt generation's embedded watermark. */
  sourceThroughOffset: string | null;
  /** The durable active generation after this operation. */
  activeGeneration: string;
  /**
   * Reducer version the outgoing generation was recorded under, or null if no
   * generation row exists for it. When this differs from `toReducerVersion` the
   * rebuild is a reducer migration, not just a fresh replay — which is the only
   * way a stream written by an older reducer is brought onto the current one.
   */
  fromReducerVersion: string | null;
  /** Reducer version the new generation was built by; always the current one. */
  toReducerVersion: string;
  equivalence: RebuildEquivalence;
  /** Every generation retained for the game, oldest first. */
  retainedGenerations: string[];
  failure?: string;
}

/** Verify the rows in the actual candidate output, not just the projection checkpoint. */
async function verifyOutput(
  streams: GameStreams,
  rebuilt: Awaited<ReturnType<typeof materializeBoard>>,
  gameId: string,
) {
  const params = { gameId, generation: rebuilt.generation };
  const address = BoardStream.at(params);
  const actual = await streams.runPromise(
    Effect.gen(function* () {
      const stream = yield* BoardStream;
      return yield* stream.collect(params);
    }),
  );
  const rows = new Map<string, string>();
  let meta;
  for (const item of actual.items) {
    if (!StreamAddress.isChange(item)) return { rowsEqual: false, watermarkEqual: false };
    const key = `${item.type}\0${item.key}`;
    if (item.type === "projectionMeta") {
      meta = StreamAddress.isUpsert(item) ? item.value : undefined;
    } else if (StreamAddress.isUpsert(item)) {
      rows.set(key, canonicalJson(item.value));
    } else rows.delete(key);
  }
  const expected = DurableState.changes(
    address,
    {},
    boardChanges(initialProjection(gameId), rebuilt.state),
  );
  const rowsEqual =
    rows.size === expected.length &&
    expected.every(
      (row) =>
        StreamAddress.isUpsert(row) &&
        rows.get(`${row.type}\0${row.key}`) === canonicalJson(row.value),
    );
  const watermarkEqual =
    meta?.id === BOARD_META_KEY &&
    meta.generation === rebuilt.generation &&
    meta.reducerVersion === BOARD_REDUCER_VERSION &&
    meta.sourceStreamId === eventStreamId(gameId) &&
    meta.sourceThroughOffset === rebuilt.sourceThroughOffset &&
    canonicalJson(meta.snapshot) === canonicalJson(rebuilt.state);
  return { rowsEqual, watermarkEqual };
}

/** Rebuild into a fresh retained stream and repoint SQLite only after independent verification. */
export async function rebuildBoardGeneration(
  deps: RebuildDeps,
  gameId: string,
  options: RebuildOptions = {},
): Promise<RebuildResult> {
  const { streams, stores } = deps;
  const game = stores.games.get(gameId);
  if (!game)
    return {
      status: "not-found",
      gameId,
      fromGeneration: "",
      toGeneration: "",
      canonicalHead: null,
      sourceThroughOffset: null,
      activeGeneration: "",
      fromReducerVersion: null,
      toReducerVersion: BOARD_REDUCER_VERSION,
      equivalence: { boardEqual: false, watermarkEqual: false },
      retainedGenerations: [],
    };
  const fromGeneration = game.generation;
  const fromReducerVersion = stores.generations.get(gameId, fromGeneration)?.reducerVersion ?? null;
  let generation = options.generation ?? nextGeneration(fromGeneration);
  if (options.generation === undefined) {
    while (stores.generations.get(gameId, generation)) generation = nextGeneration(generation);
  }
  if (generation === fromGeneration || stores.generations.get(gameId, generation)) {
    throw new Error(`Refusing to rebuild into existing generation "${generation}"`);
  }
  const params = { gameId, generation };
  // Validate the route segment before recording a candidate.
  BoardStream.at(params);
  const now = options.now ?? (() => Date.now());
  const candidate = {
    gameId,
    generation,
    streamId: boardStreamId(gameId, generation),
    reducerVersion: BOARD_REDUCER_VERSION,
    sourceThroughOffset: null,
    createdAt: now(),
  };
  stores.generations.put({ ...candidate, status: "building" });
  let canonicalHead: string | null = null;
  let sourceThroughOffset: string | null = null;
  let equivalence: RebuildEquivalence = { boardEqual: false, watermarkEqual: false };
  let status: RebuildResult["status"] = "verification-failed";
  let failure: string | undefined;
  try {
    const created = await streams.runPromise(
      Effect.flatMap(BoardStream, (board) => board.create(params)),
    );
    if (!CreateResult.$is("Created")(created))
      throw new Error(`Output for "${generation}" is not fresh`);
    const rebuilt = await (options.materialize ?? materializeBoard)(streams, gameId, generation);
    sourceThroughOffset = rebuilt.sourceThroughOffset;
    const output = await verifyOutput(streams, rebuilt, gameId);
    const { events, head } = await readCanonical(streams, eventStreamId(gameId));
    canonicalHead = events.length ? head : null;
    equivalence = {
      boardEqual:
        output.rowsEqual &&
        boardsEqual(projectionBoardView(rebuilt.state), aggregateBoardView(foldAggregate(events))),
      watermarkEqual: output.watermarkEqual && sourceThroughOffset === canonicalHead,
    };
    if (
      equivalence.boardEqual &&
      equivalence.watermarkEqual &&
      stores.games.get(gameId)?.generation === fromGeneration
    ) {
      stores.generations.activate(gameId, generation, sourceThroughOffset, now());
      status = "cutover";
    }
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }
  if (status !== "cutover")
    stores.generations.put({ ...candidate, status: "failed", sourceThroughOffset });
  return {
    status,
    gameId,
    fromGeneration,
    toGeneration: generation,
    canonicalHead,
    sourceThroughOffset,
    activeGeneration: stores.games.get(gameId)?.generation ?? fromGeneration,
    fromReducerVersion,
    toReducerVersion: BOARD_REDUCER_VERSION,
    equivalence,
    retainedGenerations: stores.generations.list(gameId).map((row) => row.generation),
    failure,
  };
}
