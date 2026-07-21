/**
 * Per-player turn notifications — a derived, rebuildable fan-out of canonical
 * history into one durable Streamsy stream per player.
 *
 * A `TurnAvailable` wake is produced exactly when control passes to a player:
 * on `GameStarted` (the first player) and on each `TurnEnded` (the next player).
 * Notifications are hints, never authoritative — an awakened agent always fetches
 * fresh `/decision` and every command is revalidated against canonical history.
 *
 * Replay-safety: each player's stream is appended under producer identity
 * `risk-turns:<gameId>:<playerId>` with `producerSeq` = the 0-based ordinal of
 * that player's wake. Re-deriving after a crash/restart re-appends the same
 * sequence, which Streamsy classifies `duplicate` — so a rebuild produces the
 * same logical wakes with no second notification. This does not use
 * `ProjectionRuntime` because wakes are sparse (most events produce none) and fan
 * out to different streams, which a per-source-event materializer does not model.
 */

import { ZERO_OFFSET } from "@streamsy/core";
import type { ProtocolStream, StreamProtocolFactory } from "@streamsy/core";

import type { GameEvent } from "../src/events.ts";
import { buildTurnId } from "../src/aggregate.ts";
import { turnStreamId } from "./names.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface TurnNotification {
  type: "TurnAvailable";
  notificationId: string;
  gameId: string;
  playerId: string;
  turnId: string;
  round: number;
  phase: "reinforce";
  /** Canonical offset of the event that made this player actionable. */
  causedBySourceOffset: string;
  decisionUrl: string;
}

interface OffsetEvent {
  event: GameEvent;
  offset: string;
}

async function readEventsWithOffsets(
  protocol: StreamProtocolFactory,
  streamId: string,
): Promise<OffsetEvent[]> {
  const got = await protocol.get(streamId);
  if (got.status !== "ok") return [];
  const out: OffsetEvent[] = [];
  let offset: string | undefined;
  for (;;) {
    const read = await got.stream.read({ offset });
    if (read.status !== "ok") break;
    for (const message of read.messages) {
      out.push({
        event: JSON.parse(decoder.decode(message.data)) as GameEvent,
        offset: message.offset,
      });
    }
    if (read.upToDate || read.messages.length === 0) break;
    offset = read.nextOffset;
  }
  return out;
}

/**
 * Pure derivation: the ordered `TurnAvailable` wakes each player should have,
 * given canonical events and their offsets. A player's wakes are in turn order.
 */
export function deriveWakes(
  gameId: string,
  events: readonly OffsetEvent[],
): Map<string, TurnNotification[]> {
  const byPlayer = new Map<string, TurnNotification[]>();
  const push = (playerId: string, round: number, causedBySourceOffset: string): void => {
    const turnId = buildTurnId(round, playerId);
    const list = byPlayer.get(playerId) ?? [];
    list.push({
      type: "TurnAvailable",
      notificationId: `turn:${gameId}:${playerId}:${round}`,
      gameId,
      playerId,
      turnId,
      round,
      phase: "reinforce",
      causedBySourceOffset,
      decisionUrl: `/v1/games/${gameId}/decision`,
    });
    byPlayer.set(playerId, list);
  };

  for (const { event, offset } of events) {
    if (event.type === "GameStarted") {
      const first = event.turnOrder[0];
      if (first) push(first, event.round, offset);
    } else if (event.type === "TurnEnded") {
      push(event.nextPlayerId, event.round, offset);
    }
  }
  return byPlayer;
}

async function ensureStream(
  protocol: StreamProtocolFactory,
  streamId: string,
): Promise<ProtocolStream> {
  const got = await protocol.get(streamId);
  if (got.status === "ok") return got.stream;
  const created = await protocol.create(streamId, { contentType: "application/json" });
  if (created.status === "created" || created.status === "exists") return created.stream;
  throw new Error(`cannot open turn stream ${streamId}: ${created.status}`);
}

async function streamTail(stream: ProtocolStream): Promise<{ count: number; tail: string }> {
  let count = 0;
  let tail = ZERO_OFFSET;
  let offset: string | undefined;
  for (;;) {
    const read = await stream.read({ offset });
    if (read.status !== "ok") break;
    count += read.messages.length;
    if (read.messages.length > 0) tail = read.messages[read.messages.length - 1]!.offset;
    if (read.upToDate || read.messages.length === 0) break;
    offset = read.nextOffset;
  }
  return { count, tail };
}

/**
 * Idempotently append any not-yet-produced wakes for every player. Safe to call
 * after each accepted command and safe to re-run after a restart or rebuild.
 */
export async function catchUpTurns(protocol: StreamProtocolFactory, gameId: string): Promise<void> {
  const events = await readEventsWithOffsets(protocol, `games/${gameId}/events`);
  const wakesByPlayer = deriveWakes(gameId, events);

  for (const [playerId, wakes] of wakesByPlayer) {
    const streamId = turnStreamId(gameId, playerId);
    const stream = await ensureStream(protocol, streamId);
    let { count, tail } = await streamTail(stream);
    for (let seq = count; seq < wakes.length; seq += 1) {
      const result = await stream.append({
        data: encoder.encode(JSON.stringify(wakes[seq])),
        contentType: "application/json",
        producer: {
          producerId: `risk-turns:${gameId}:${playerId}`,
          producerEpoch: 1,
          producerSeq: seq,
        },
        expectedOffset: tail,
      });
      if (result.status === "appended") {
        tail = result.offset;
        continue;
      }
      if (result.status === "duplicate") {
        // Already produced (concurrent notifier or rebuild) — reload and retry.
        const reloaded = await streamTail(stream);
        count = reloaded.count;
        tail = reloaded.tail;
        seq = count - 1;
        continue;
      }
      if (result.status === "conflict" && result.conflictReason === "expected-offset") {
        const reloaded = await streamTail(stream);
        count = reloaded.count;
        tail = reloaded.tail;
        seq = count - 1;
        continue;
      }
      throw new Error(`turn notification append failed for ${streamId}: ${result.status}`);
    }
  }
}

export interface TurnRead {
  notifications: TurnNotification[];
  cursor: string;
  upToDate: boolean;
}

/**
 * Read a player's turn notifications after `cursor` (after-exclusive). When none
 * are available and `waitMs > 0`, long-poll for the next wake.
 */
export async function readTurns(
  protocol: StreamProtocolFactory,
  gameId: string,
  playerId: string,
  options: { cursor?: string; waitMs?: number; signal?: AbortSignal } = {},
): Promise<TurnRead> {
  const streamId = turnStreamId(gameId, playerId);
  const stream = await ensureStream(protocol, streamId);
  const offset = options.cursor ?? ZERO_OFFSET;

  const read = await stream.read({ offset });
  if (read.status === "ok" && read.messages.length > 0) {
    return {
      notifications: read.messages.map(
        (m) => JSON.parse(decoder.decode(m.data)) as TurnNotification,
      ),
      cursor: read.nextOffset,
      upToDate: read.upToDate,
    };
  }

  if (options.waitMs && options.waitMs > 0) {
    const live = await stream.readLive({ offset, mode: "long-poll", signal: options.signal });
    if (live.status === "ok" && live.messages.length > 0) {
      return {
        notifications: live.messages.map(
          (m) => JSON.parse(decoder.decode(m.data)) as TurnNotification,
        ),
        cursor: live.nextOffset,
        upToDate: live.upToDate,
      };
    }
    return {
      notifications: [],
      cursor: live.status === "not-supported" ? offset : live.nextOffset,
      upToDate: true,
    };
  }

  return {
    notifications: [],
    cursor: read.status === "ok" ? read.nextOffset : offset,
    upToDate: true,
  };
}
