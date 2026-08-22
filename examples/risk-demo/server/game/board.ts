/* oxlint-disable effecttsgo/async-function -- This module preserves a public Promise compatibility facade over protocol/runtime-owned application work. */
/**
 * `Hex Domination` board materialization for `GET /board` and the decision
 * watermark.
 *
 * Each call runs one bounded `ivm-mesh` catch-up and returns the state that
 * catch-up recovered from the durable board stream. There is no cached board
 * state: the host reuses one runtime, while the mesh recovers
 * `{ rows, lineage, next sequence }`
 * from the target on every invocation, so two servers — or one server either
 * side of a restart — cannot disagree about the board because one of them
 * happens to be holding a warmer copy.
 *
 * The cost of that is honest and worth naming: recovery is O(board history) per
 * call, because the incubating mesh has no bounded recovery anchor yet. For a
 * single game's board that is a small scan; it is not a shape to carry into a
 * large history without a snapshot anchor.
 */

import { directProtocolClient, type StreamProtocolFactory } from "@streamsy/core";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/experimental/effect";
import {
  catchUpState,
  DerivedRecoveryLive,
  DerivedStateHistoryLive,
} from "@streamsy/experimental/ivm-mesh";
import { Layer, ManagedRuntime } from "effect";

import type { GameEvent } from "../../src/domain/events.ts";
import type { ProjectionState } from "../../src/board/projection.ts";
import { createBoardMesh, type BoardMaterialized, type BoardMesh } from "../../src/board/mesh.ts";
import { BOARD_GENERATION, boardStreamId, eventStreamId } from "./names.ts";

const BoardLayer = Layer.mergeAll(DerivedRecoveryLive, DerivedStateHistoryLive).pipe(
  Layer.provideMerge(Layer.mergeAll(ReadStreamsLive, AppendStreamsLive)),
);

/** One Effect runtime and protocol client, owned for the host's lifetime. */
export interface BoardRuntimeCache {
  readonly client: ReturnType<typeof directProtocolClient>;
  readonly runtime: ManagedRuntime.ManagedRuntime<Layer.Success<typeof BoardLayer>, never>;
}

export function createBoardRuntimeCache(protocol: StreamProtocolFactory): BoardRuntimeCache {
  return {
    client: directProtocolClient(protocol),
    runtime: ManagedRuntime.make(BoardLayer),
  };
}

/**
 * How many further bounded catch-up passes one board read will run before
 * giving up. The limits in `BOARD_CATCHUP_LIMITS` are generous enough that a
 * game reaching this has something wrong with it, not merely a long history.
 */
const MAX_CATCHUP_PASSES = 8;

export interface MaterializedBoard {
  state: ProjectionState;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
}

// oxlint-disable-next-line effecttsgo/extends-native-error -- BoardProjectionError is the documented rejected-Promise error returned by this Promise facade.
export class BoardProjectionError extends Error {
  constructor(
    readonly status: string,
    message: string,
  ) {
    super(message);
    this.name = "BoardProjectionError";
  }
}

/**
 * Catch the board projection up to the canonical head and return it, together
 * with the causal watermark the decision resource reports.
 */
export async function materializeBoard(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCache,
  gameId: string,
  generation: string = BOARD_GENERATION,
): Promise<MaterializedBoard> {
  const sourceStreamId = eventStreamId(gameId);
  const outputStreamId = boardStreamId(gameId, generation);
  const mesh = await createBoardMesh({
    gameId,
    client: cache.client,
    sourceStreamId,
    outputStreamId,
    generation,
  });
  await ensureStream(protocol, outputStreamId);

  // `limit-reached` means the invocation's budget ran out with source left to
  // read, and its checkpoint describes a *partial* board. Returning it would be
  // the worst kind of wrong here: the board would look complete, and its
  // watermark would be handed to `syncedThrough` as proof of a causal position
  // the projection had not actually reached. So further bounded passes are run,
  // and if the backlog still has not drained the read fails rather than
  // reporting a board nobody asked for.
  //
  // Today this is a guard rather than a live path: a catch-up read returns
  // everything unread as one delivery batch, so one invocation covers one
  // boundary and `limit-reached` — which needs a second boundary to refuse —
  // cannot arise. It is kept because that is a property of the current reader,
  // not of this code, and because the failure it prevents is silent.
  let result = await cache.runtime.runPromise(runBoardCatchUp(mesh));
  for (
    let attempt = 1;
    result.status === "limit-reached" && attempt <= MAX_CATCHUP_PASSES;
    attempt += 1
  ) {
    result = await cache.runtime.runPromise(runBoardCatchUp(mesh));
  }

  // `missing` on the source is an ordinary state: a game whose canonical stream
  // has no events yet has an empty board, not a broken one.
  if (result.status === "missing" && result.stream === "source") {
    return {
      state: mesh.initial.state,
      sourceStreamId,
      sourceThroughOffset: null,
      generation,
    };
  }
  if (result.status === "limit-reached") {
    throw new BoardProjectionError(
      "backlog",
      `board projection still had source to read after ${MAX_CATCHUP_PASSES + 1} bounded passes`,
    );
  }
  if (result.status !== "caught-up") {
    throw new BoardProjectionError(
      result.status,
      `board projection did not catch up: ${result.status}`,
    );
  }

  // Snapshot/lineage agreement is enforced by `mesh.validateRecovered`, which
  // runs inside `catchUp` before any source is read. Checking it here instead
  // would be too late to mean anything: by then a mismatched resume has already
  // re-applied events and committed a boundary that makes the two agree.
  return {
    state: result.state.state,
    sourceStreamId,
    sourceThroughOffset: result.checkpoint.sourceThrough ?? null,
    generation,
  };
}

export function runBoardCatchUp(mesh: BoardMesh) {
  return catchUpState<BoardMaterialized, GameEvent>({
    source: mesh.source,
    target: mesh.target,
    lane: mesh.lane,
    limits: mesh.limits,
    initial: mesh.initial,
    restore: (initial, facts) => mesh.restore(initial, facts),
    validateRecovered: (checkpoint) => mesh.validateRecovered(checkpoint),
    decode: (batch) => mesh.decode(batch),
    step: (prior, events, boundary) => ({ facts: mesh.reduce(events, boundary, prior) }),
  });
}

/** The mesh recovers from durable output, so the target must exist to be read. */
async function ensureStream(protocol: StreamProtocolFactory, streamId: string): Promise<void> {
  const existing = await protocol.get(streamId);
  if (existing.status === "ok") return;
  const created = await protocol.create(streamId, { contentType: "application/json" });
  if (created.status !== "created" && created.status !== "exists") {
    throw new BoardProjectionError(created.status, `cannot open board stream: ${created.status}`);
  }
}
