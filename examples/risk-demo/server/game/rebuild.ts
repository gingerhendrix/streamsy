/**
 * Board-projection generation rebuild + durable cutover (Batch 5).
 *
 * A generation is a *separate, rebuildable* Durable State board stream
 * (`games/<id>/projections/board/<generation>`). Rebuilding replays the whole
 * canonical event log into a fresh generation through the same replay-safe
 * {@link ProjectionRuntime}, verifies the result against the authoritative
 * aggregate fold (logical board equivalence + identical canonical
 * `sourceThroughOffset`), and only then atomically repoints the durable active
 * generation. Verification failure leaves the old active generation untouched
 * and usable; old generations are retained, never deleted, so a cutover is
 * reversible.
 *
 * Both rulesets take the same route. Only the (reducer, adapter, equivalence
 * view) triple differs, which is exactly what a {@link RebuildPlan} carries — so
 * a v2 game rebuilds under the v2 reducer version and its own generation lineage
 * without any v1 projection history being touched.
 */

import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";
import type { ProjectionAdapter } from "@streamsy/experimental/projection";

import type { GameEvent } from "../../src/domain/events.ts";
import type { GameEventV2 } from "../../src/domain/events-v2.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import { foldAggregateV2 } from "../../src/domain/aggregate-v2.ts";
import {
  aggregateBoardView,
  boardsEqual,
  projectionBoardView,
  type ProjectionState,
} from "../../src/board/projection.ts";
import {
  aggregateBoardViewV2,
  boardsEqualV2,
  projectionBoardViewV2,
  type ProjectionStateV2,
} from "../../src/board/projection-v2.ts";
import {
  BOARD_REDUCER_VERSION,
  createBoardProjectionAdapter,
  type BoardProjectionAdapterOptions,
} from "../../src/board/board-projection.ts";
import {
  BOARD_REDUCER_VERSION_V2,
  createBoardProjectionAdapterV2,
  type BoardProjectionAdapterOptionsV2,
} from "../../src/board/board-projection-v2.ts";
import { isRulesetV2, readCanonical, readCanonicalV2 } from "./command-service.ts";
import { boardStreamId, eventStreamId, nextGeneration } from "./names.ts";
import type { GameRow, Stores } from "../persistence/stores.ts";

export interface RebuildDeps {
  protocol: StreamProtocolFactory;
  stores: Stores;
}

export interface RebuildOptions {
  /** Target generation id. Defaults to the next id after the active one. */
  generation?: string;
  now?: () => number;
  /** Adapter factory (overridable so a corrupt reducer can be exercised in tests). */
  makeAdapter?: (
    options: BoardProjectionAdapterOptions,
  ) => ProjectionAdapter<ProjectionState, GameEvent>;
  /** The v2 equivalent of {@link makeAdapter}. */
  makeAdapterV2?: (
    options: BoardProjectionAdapterOptionsV2,
  ) => ProjectionAdapter<ProjectionStateV2, GameEventV2>;
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
  equivalence: RebuildEquivalence;
  /** Every generation retained for the game, oldest first. */
  retainedGenerations: string[];
}

/** The ruleset-specific half of a rebuild: reducer identity plus verification. */
interface RebuildPlan<State, Event> {
  reducerVersion: string;
  adapter(options: BoardProjectionAdapterOptionsV2): ProjectionAdapter<State, Event>;
  readCanonical(
    protocol: StreamProtocolFactory,
    streamId: string,
  ): Promise<{ events: Event[]; head: string }>;
  boardEqual(state: State, events: readonly Event[]): boolean;
}

function v1Plan(options: RebuildOptions): RebuildPlan<ProjectionState, GameEvent> {
  return {
    reducerVersion: BOARD_REDUCER_VERSION,
    adapter: options.makeAdapter ?? createBoardProjectionAdapter,
    readCanonical: (protocol, streamId) => readCanonical(protocol, streamId),
    boardEqual: (state, events) =>
      boardsEqual(projectionBoardView(state), aggregateBoardView(foldAggregate(events))),
  };
}

function v2Plan(options: RebuildOptions): RebuildPlan<ProjectionStateV2, GameEventV2> {
  return {
    reducerVersion: BOARD_REDUCER_VERSION_V2,
    adapter: options.makeAdapterV2 ?? createBoardProjectionAdapterV2,
    readCanonical: (protocol, streamId) => readCanonicalV2(protocol, streamId),
    boardEqual: (state, events) =>
      boardsEqualV2(projectionBoardViewV2(state), aggregateBoardViewV2(foldAggregateV2(events))),
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

  const runtime = new ProjectionRuntime({
    protocol: deps.protocol,
    adapter: plan.adapter({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId,
      generation: toGeneration,
    }),
  });
  await runtime.catchUp();
  const status = await runtime.status();

  // Verify against the authoritative aggregate fold at the canonical head.
  const { events, head } = await plan.readCanonical(deps.protocol, eventStreamId(gameId));
  const rebuiltWatermark = status.sourceThroughOffset;
  const canonicalHead = events.length > 0 ? head : null;

  const boardEqual = plan.boardEqual(runtime.currentState(), events);
  const watermarkEqual = (rebuiltWatermark ?? null) === canonicalHead;
  const equivalence: RebuildEquivalence = { boardEqual, watermarkEqual };

  const base = {
    gameId,
    fromGeneration,
    toGeneration,
    canonicalHead,
    sourceThroughOffset: rebuiltWatermark,
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
  return isRulesetV2(game.ruleset)
    ? runRebuild(deps, game, options, v2Plan(options))
    : runRebuild(deps, game, options, v1Plan(options));
}
