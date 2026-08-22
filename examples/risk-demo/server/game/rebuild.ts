/* oxlint-disable effecttsgo/async-function -- This module preserves a public Promise compatibility facade over protocol/runtime-owned application work. */
/* oxlint-disable effecttsgo/global-date -- Projection generation ids intentionally include the host wall-clock at this rebuild facade boundary. */
/**
 * Board-projection generation rebuild and durable cutover.
 *
 * A generation is a *separate, rebuildable* Durable State board stream
 * (`games/<id>/projections/board/<generation>`). Rebuilding replays the whole
 * canonical event log into a fresh generation through the same bounded mesh
 * `catchUp`, verifies the result against the authoritative
 * aggregate fold (logical board equivalence + identical canonical
 * `sourceThroughOffset`), and only then atomically repoints the durable active
 * generation. Verification failure leaves the old active generation untouched
 * and usable; old generations are retained, never deleted, so a cutover is
 * reversible.
 *
 * This is a first-class durability demonstration: a replacement projection is
 * verified before the active pointer changes, and every generation remains
 * available for inspection.
 */

import type { StreamProtocolFactory } from "@streamsy/core";
import { catchUpState } from "@streamsy/experimental/ivm-mesh";

import type { GameEvent } from "../../src/domain/events.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import {
  aggregateBoardView,
  boardsEqual,
  projectionBoardView,
  type ProjectionState,
} from "../../src/board/projection.ts";
import { BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";
import { createBoardMesh, type BoardMesh, type BoardMeshOptions } from "../../src/board/mesh.ts";
import { readCanonical } from "./command-service.ts";
import { boardStreamId, eventStreamId, nextGeneration } from "./names.ts";
import type { GameRow, Stores } from "../persistence/stores.ts";
import type { BoardRuntimeCache } from "./board.ts";

export interface RebuildDeps {
  protocol: StreamProtocolFactory;
  stores: Stores;
  boardRuntime: BoardRuntimeCache;
}

export interface RebuildOptions {
  /** Target generation id. Defaults to the next id after the active one. */
  generation?: string;
  now?: () => number;
  /** Mesh factory (overridable so a corrupt reducer can be exercised in tests). */
  makeMesh?: (options: BoardMeshOptions) => Promise<BoardMesh>;
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
}

interface RebuildPlan<State, Event> {
  reducerVersion: string;
  mesh(options: BoardMeshOptions): Promise<BoardMesh>;
  readCanonical(
    protocol: StreamProtocolFactory,
    streamId: string,
  ): Promise<{ events: Event[]; head: string }>;
  boardEqual(state: State, events: readonly Event[]): boolean;
}

function rebuildPlan(options: RebuildOptions): RebuildPlan<ProjectionState, GameEvent> {
  return {
    reducerVersion: BOARD_REDUCER_VERSION,
    mesh: options.makeMesh ?? createBoardMesh,
    readCanonical: (protocol, streamId) => readCanonical(protocol, streamId),
    boardEqual: (state, events) =>
      boardsEqual(projectionBoardView(state), aggregateBoardView(foldAggregate(events))),
  };
}

function notFound(gameId: string): RebuildResult {
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
}

async function runRebuild<State, Event>(
  deps: RebuildDeps,
  game: GameRow,
  options: RebuildOptions,
  plan: RebuildPlan<State, Event>,
): Promise<RebuildResult> {
  const now = options.now ?? (() => Date.now());
  const gameId = game.gameId;
  const fromGeneration = game.generation;
  const toGeneration = options.generation ?? nextGeneration(fromGeneration);
  if (toGeneration === fromGeneration) {
    throw new Error(`refusing to rebuild into the active generation "${fromGeneration}"`);
  }
  const outputStreamId = boardStreamId(gameId, toGeneration);

  // Record the new generation as "building" before any output is written.
  deps.stores.generations.put({
    gameId,
    generation: toGeneration,
    streamId: outputStreamId,
    reducerVersion: plan.reducerVersion,
    status: "building",
    sourceThroughOffset: null,
    createdAt: now(),
  });

  // A generation is a fresh, empty output stream built entirely by the current
  // reducer; the mesh recovers from it, so it must exist before catch-up.
  const created = await deps.protocol.create(outputStreamId, {
    contentType: "application/json",
  });
  if (created.status !== "created" && created.status !== "exists") {
    throw new Error(`cannot open rebuild target stream: ${created.status}`);
  }
  const mesh = await plan.mesh({
    gameId,
    client: deps.boardRuntime.client,
    sourceStreamId: eventStreamId(gameId),
    outputStreamId,
    generation: toGeneration,
  });
  const rebuilt = await deps.boardRuntime.runtime.runPromise(
    catchUpState({
      source: mesh.source,
      target: mesh.target,
      lane: mesh.lane,
      limits: mesh.limits,
      initial: mesh.initial,
      restore: (initial, facts) => mesh.restore(initial, facts),
      validateRecovered: (checkpoint) => mesh.validateRecovered(checkpoint),
      decode: (batch) => mesh.decode(batch),
      step: (prior, items, boundary) => ({ facts: mesh.reduce(items, boundary, prior) }),
    }),
  );

  // Verify against the authoritative aggregate fold at the canonical head.
  const { events, head } = await plan.readCanonical(deps.protocol, eventStreamId(gameId));
  const canonicalHead = events.length > 0 ? head : null;
  // A rebuild that did not reach `caught-up` verifies against nothing and must
  // fail verification rather than cut over on a partial replay.
  const complete = rebuilt.status === "caught-up";
  const recovered = complete && "checkpoint" in rebuilt ? rebuilt : undefined;
  const rebuiltWatermark = recovered?.checkpoint.sourceThrough ?? null;
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The generic projection library returns the same State supplied by this caller; its API has no runtime State schema parameter.
  const rebuiltState = (recovered?.state ?? mesh.initial).state as State;

  const boardEqual = complete && plan.boardEqual(rebuiltState, events);
  const watermarkEqual = (rebuiltWatermark ?? null) === canonicalHead;
  const equivalence: RebuildEquivalence = { boardEqual, watermarkEqual };

  const base = {
    gameId,
    fromGeneration,
    toGeneration,
    canonicalHead,
    sourceThroughOffset: rebuiltWatermark,
    fromReducerVersion: deps.stores.generations.get(gameId, fromGeneration)?.reducerVersion ?? null,
    toReducerVersion: plan.reducerVersion,
    equivalence,
  };

  if (boardEqual && watermarkEqual) {
    // Cut over: retire old, activate new, repoint the game — one transaction.
    deps.stores.generations.activate(gameId, toGeneration, rebuiltWatermark, now());
    return {
      ...base,
      status: "cutover",
      activeGeneration: toGeneration,
      retainedGenerations: deps.stores.generations.list(gameId).map((g) => g.generation),
    };
  }

  // Verification failed: mark the new generation failed and keep the old active.
  deps.stores.generations.put({
    gameId,
    generation: toGeneration,
    streamId: outputStreamId,
    reducerVersion: plan.reducerVersion,
    status: "failed",
    sourceThroughOffset: rebuiltWatermark,
    createdAt: now(),
  });
  return {
    ...base,
    status: "verification-failed",
    activeGeneration: fromGeneration,
    retainedGenerations: deps.stores.generations.list(gameId).map((g) => g.generation),
  };
}

/**
 * Rebuild `gameId`'s board into a fresh generation and, if it verifies, cut the
 * durable active pointer over to it. Idempotent-safe to re-run: a re-run targets
 * the next fresh generation id.
 */
export async function rebuildBoardGeneration(
  deps: RebuildDeps,
  gameId: string,
  options: RebuildOptions = {},
): Promise<RebuildResult> {
  const game = deps.stores.games.get(gameId);
  if (!game) return notFound(gameId);
  return runRebuild(deps, game, options, rebuildPlan(options));
}
