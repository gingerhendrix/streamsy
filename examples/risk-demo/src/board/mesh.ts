/**
 * Hex Domination's board projection, expressed on the mesh primitives.
 *
 * The board is a stateful projection: to emit a row *change* it has to know the
 * rows it previously wrote, so it can tell an insert from an update and notice a
 * territory row that has gone away. `ivm-mesh` supplies that through a
 * {@link DerivedStateFold}, which rebuilds the projection state from the durable
 * board stream during the recovery scan `catchUp` already performs. Nothing is
 * cached in this process: a restarted server reduces against what it wrote.
 *
 * ## What is recovered, and from where
 *
 * The fold reads exactly one row — the application's own `projectionMeta`, which
 * carries a complete `ProjectionState` snapshot and the applied-event ordinal.
 * That row predates the mesh and is kept deliberately: the UI, the decision
 * resource, and generation verification all read it. The framework's reserved
 * `__streamsy.mesh.lineage.v1` row sits alongside it in the same transaction and
 * owns lineage and producer sequence. They answer different questions and are
 * not merged.
 *
 * ## Transactions and acknowledgement
 *
 * One source delivery boundary becomes one State transaction holding every row
 * change it implies plus the lineage row, so the board and its causal position
 * can never commit apart. A boundary can never split one command's events (a
 * canonical append is atomic and a read returns whole messages up to the durable
 * head), which is what lets a transaction be named after a command — but a
 * boundary may well contain *several* commands when catching up on a backlog,
 * so changes carry a txid each rather than the transaction carrying one. See
 * {@link boardProjectionTxId}.
 */

import { type JsonValue, type StreamProtocolClient } from "@streamsy/core";
import { bindStream, type StreamBinding } from "@streamsy/experimental/binding";
import { streamIdentity, type StreamIdentity } from "@streamsy/experimental/causal";
import {
  deriveProducerLane,
  type CatchUpLimits,
  type ProducerLane,
  type ProjectionBoundary,
} from "@streamsy/experimental/ivm-mesh";

import { GameEvent, type GameEvent as GameEventType } from "../domain/events.ts";
import { Schema } from "effect";
import { boardProjectionTxId } from "./transaction.ts";
import {
  BOARD_META_KEY,
  BOARD_META_TYPE,
  BOARD_REDUCER_VERSION,
  boardRows,
  type BoardProjectionMetaRow,
  type DurableStateProjectionRow,
} from "./board-projection.ts";
import { initialProjection, projectEvent, type ProjectionState } from "./projection.ts";
import {
  BoardMetaFactSchema,
  BoardProjectionMetaSchema,
  DurableBoardFactSchema,
  ProjectedCombatSchema,
  ProjectedContinentSchema,
  ProjectedGameSchema,
  ProjectedHexSchema,
  ProjectedMoveSchema,
  ProjectedPlayerSchema,
  ProjectedTerritorySchema,
  ProjectedTurnSchema,
} from "./schemas.ts";

/**
 * The epoch is fixed configuration for a board generation, never claimed and
 * never bumped on restart. A generation's output is immutable; a *new* board is
 * a new generation with its own stream, which is what `rebuild` produces.
 */
export const BOARD_PRODUCER_EPOCH = 1;

/** Bounded per-invocation work. Generous: a game's canonical log is small. */
export const BOARD_CATCHUP_LIMITS: CatchUpLimits = {
  maxItems: 10_000,
  maxPages: 1_000,
  maxBatches: 1_000,
  maxBytes: 16 * 1024 * 1024,
};

/** Projection state as recovered from the durable board stream. */
export interface BoardMaterialized {
  readonly state: ProjectionState;
  /** 0-based ordinal of the last applied canonical event, or -1 if none. */
  readonly sourceSeq: number;
}

export interface BoardMeshOptions {
  gameId: string;
  /** Caller-owned client. The caller must close it when its host or test ends. */
  client: StreamProtocolClient;
  sourceStreamId: string;
  outputStreamId: string;
  processorId?: string;
  generation: string;
  /** Test seam; production always uses {@link BOARD_CATCHUP_LIMITS}. */
  limits?: CatchUpLimits;
}

export interface BoardMesh {
  readonly gameId: string;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly limits: CatchUpLimits;
  readonly initial: BoardMaterialized;
  /** Test-facing fold view of the same durable materializer. */
  readonly fold: {
    initial(): BoardMaterialized;
    apply(state: BoardMaterialized, fact: JsonValue): BoardMaterialized;
  };
  restore(initial: BoardMaterialized, facts: readonly JsonValue[]): BoardMaterialized;
  validateRecovered(checkpoint: { sourceThrough?: string; state: BoardMaterialized }): void;
  decode(batch: { kind: string; items?: readonly JsonValue[] }): readonly GameEventType[];
  reduce(
    events: readonly GameEventType[],
    boundary: ProjectionBoundary,
    prior: BoardMaterialized,
  ): JsonValue[];
}

/**
 * Mesh identities are assigned by the mesh and are deliberately *not* the
 * application's stream ids. Naming them after what they mean keeps a stream id
 * change from silently becoming a causal identity change.
 */
export function boardSourceIdentity(gameId: string): StreamIdentity {
  return streamIdentity(`risk.canonical.${gameId}`);
}

export function boardTargetIdentity(gameId: string, generation: string): StreamIdentity {
  return streamIdentity(`risk.board.${gameId}.${generation}`);
}

function initialMaterialized(gameId: string): BoardMaterialized {
  return { state: initialProjection(gameId), sourceSeq: -1 };
}

/**
 * Rebuild projection state from the durable board.
 *
 * Only the `projectionMeta` snapshot is read. The individual row facts are the
 * *consequence* of that state rather than an independent record of it, so
 * folding them as well would be a second, weaker derivation of the same thing.
 */
function restoreBoard(
  options: BoardMeshOptions,
  initial: BoardMaterialized,
  facts: readonly JsonValue[],
): BoardMaterialized {
  const { generation, sourceStreamId } = options;
  let prior = initial;
  const decodeMetaFact = Schema.decodeUnknownOption(BoardMetaFactSchema);
  for (const fact of facts) {
    if (typeof fact !== "object" || fact === null || Array.isArray(fact)) continue;
    if (
      Reflect.get(fact, "type") !== BOARD_META_TYPE ||
      Reflect.get(fact, "key") !== BOARD_META_KEY
    )
      continue;
    const decoded = decodeMetaFact(fact);
    if (decoded._tag === "None") throw new Error("board projectionMeta row is malformed");
    const value = decoded.value.value;
    // The checkpoint row is the only thing standing between durable output and
    // this reducer's state, so check that it actually belongs here rather than
    // trusting the collection name. A row from another generation, reducer, or
    // game would otherwise be adopted silently as this board's state.
    if (value.generation !== generation) {
      throw new Error(
        `board projectionMeta belongs to generation "${value.generation}", not "${generation}"`,
      );
    }
    if (value.reducerVersion !== BOARD_REDUCER_VERSION) {
      throw new Error(
        `board projectionMeta was written by reducer "${value.reducerVersion}", not ` +
          `"${BOARD_REDUCER_VERSION}" — this generation needs a rebuild`,
      );
    }
    if (value.sourceStreamId !== sourceStreamId) {
      throw new Error(
        `board projectionMeta projects "${value.sourceStreamId}", not "${sourceStreamId}"`,
      );
    }
    if (value.sourceSeq < 0) {
      throw new Error("board projectionMeta has no applied-event ordinal");
    }
    // Ordinals count canonical events, so they only ever move forwards.
    if (value.sourceSeq <= prior.sourceSeq) {
      throw new Error(
        `board projectionMeta ordinal ${value.sourceSeq} does not advance past ${prior.sourceSeq}`,
      );
    }
    if (value.snapshot.sourceThroughOffset !== value.sourceThroughOffset) {
      throw new Error("board projectionMeta disagrees with the snapshot it carries");
    }
    prior = { state: value.snapshot, sourceSeq: value.sourceSeq };
  }
  return prior;
}

function rowKey(row: DurableStateProjectionRow): string {
  return `${row.type}\0${row.key}`;
}

function equal(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function encodeRowValue(type: string, value: unknown): JsonValue {
  const schema = {
    game: ProjectedGameSchema,
    player: ProjectedPlayerSchema,
    hex: ProjectedHexSchema,
    territory: ProjectedTerritorySchema,
    continent: ProjectedContinentSchema,
    turn: ProjectedTurnSchema,
    combat: ProjectedCombatSchema,
    move: ProjectedMoveSchema,
  }[type];
  if (!schema) throw new Error(`unknown board row type ${type}`);
  return Schema.encodeUnknownSync(schema)(value);
}

/**
 * The row difference between two projection states.
 *
 * `turn` and `combat` are zero-or-one collections: omitting the row is what
 * makes this emit a delete, which is how a resolved combat clears.
 */
function diffRows(
  previous: ProjectionState,
  next: ProjectionState,
  offset: string,
  txid: string,
): JsonValue[] {
  const before = new Map(boardRows(previous).map((row) => [rowKey(row), row]));
  const changes: JsonValue[] = [];
  const encodeFact = Schema.encodeUnknownSync(DurableBoardFactSchema);
  for (const row of boardRows(next)) {
    const key = rowKey(row);
    const prior = before.get(key);
    const value = encodeRowValue(row.type, row.value);
    if (!prior) {
      changes.push(encodeFact({ ...row, value, headers: { operation: "insert", offset, txid } }));
    } else if (!equal(prior.value, row.value)) {
      changes.push(encodeFact({ ...row, value, headers: { operation: "update", offset, txid } }));
    }
    before.delete(key);
  }
  for (const removed of before.values()) {
    changes.push(
      encodeFact({
        type: removed.type,
        key: removed.key,
        value: null,
        old_value: encodeRowValue(removed.type, removed.value),
        headers: { operation: "delete", offset, txid },
      }),
    );
  }
  return changes;
}

/** Compose every piece `catchUp` needs to maintain one board generation. */
// oxlint-disable-next-line effecttsgo/async-function -- The mesh library exposes Promise-native construction and this exported compatibility facade preserves it.
export async function createBoardMesh(options: BoardMeshOptions): Promise<BoardMesh> {
  const client = options.client;
  const sourceIdentity = boardSourceIdentity(options.gameId);
  const targetIdentity = boardTargetIdentity(options.gameId, options.generation);
  const lane = await deriveProducerLane({
    processorId: options.processorId ?? `risk-board:${options.gameId}`,
    processorVersion: BOARD_REDUCER_VERSION,
    outputGeneration: options.generation,
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: BOARD_PRODUCER_EPOCH,
  });

  return {
    gameId: options.gameId,
    source: bindStream({
      identity: sourceIdentity,
      client,
      streamId: options.sourceStreamId,
    }),
    target: bindStream({
      identity: targetIdentity,
      client,
      streamId: options.outputStreamId,
    }),
    lane,
    limits: options.limits ?? BOARD_CATCHUP_LIMITS,
    initial: initialMaterialized(options.gameId),
    fold: {
      initial: () => initialMaterialized(options.gameId),
      apply: (state, fact) => restoreBoard(options, state, [fact]),
    },
    restore: (initial, facts) => restoreBoard(options, initial, facts),

    /**
     * The two readings of the board's position must agree before it resumes.
     *
     * `projectionMeta.sourceThroughOffset` and the reserved lineage row are
     * written in one transaction, so they can only disagree if something went
     * wrong. It matters that this is checked *here* — after recovery, before the
     * source is read — rather than after a catch-up. Recovery resumes from the
     * lineage position while reducing from the recovered snapshot, so a snapshot
     * that had already moved past the lineage would silently re-apply the events
     * in between and then commit a boundary whose lineage and snapshot agree,
     * turning the inconsistency into a plausible-looking board.
     */
    validateRecovered(checkpoint) {
      const lineage = checkpoint.sourceThrough ?? null;
      const snapshot = checkpoint.state.state.sourceThroughOffset ?? null;
      if (snapshot !== lineage) {
        throw new Error(
          `board snapshot is at ${snapshot ?? "∅"} but mesh lineage is at ${lineage ?? "∅"}`,
        );
      }
    },

    decode(batch) {
      if (batch.kind !== "json" || !batch.items) {
        throw new Error("canonical game stream must be JSON");
      }
      return Schema.decodeUnknownSync(Schema.Array(GameEvent))(batch.items);
    },

    reduce(events, boundary, prior) {
      const offset = boundary.source.position;
      let state = prior.state;
      let sourceSeq = prior.sourceSeq;
      const facts: JsonValue[] = [];
      for (const event of events) {
        sourceSeq += 1;
        const next = projectEvent(state, event, offset, sourceSeq);
        facts.push(...diffRows(state, next, offset, boardProjectionTxId(event.commandId)));
        state = next;
      }
      const meta: BoardProjectionMetaRow = {
        sourceStreamId: options.sourceStreamId,
        sourceThroughOffset: offset,
        sourceSeq,
        generation: options.generation,
        reducerVersion: BOARD_REDUCER_VERSION,
        snapshot: state,
      };
      const encodedMeta = Schema.encodeUnknownSync(BoardProjectionMetaSchema)(meta);
      facts.push({
        type: BOARD_META_TYPE,
        key: BOARD_META_KEY,
        value: encodedMeta,
        // Upsert: the first transition of a generation creates this row.
        headers: {
          operation: "upsert",
          offset,
          txid: boardProjectionTxId(events.at(-1)!.commandId),
        },
      });
      return facts;
    },
  };
}
