/** Risk bindings for replay-safe derived per-player action streams. */
import type { StreamProtocolFactory } from "@streamsy/core";
import { catchUpDerived, readDerived } from "@streamsy/experimental/derived";
import type { JsonCodec } from "@streamsy/json";

import { buildTurnId } from "../../src/domain/aggregate.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { GameEventV2 } from "../../src/domain/events-v2.ts";
import { eventStreamId, turnStreamId } from "./names.ts";

export interface TurnNotification {
  type: "TurnAvailable";
  notificationId: string;
  gameId: string;
  playerId: string;
  turnId: string;
  round: number;
  phase: "reinforce";
  causedBySourceOffset: string;
  decisionUrl: string;
}

/**
 * The `risk-demo-v2` out-of-turn wake: a defender — human *or* agent — has a
 * combat waiting on their roll until `deadlineAt`.
 *
 * Like `TurnAvailable` this is a derived, rebuildable hint, not a correctness
 * channel. A browser learns about pending combat from projected state; an agent
 * harness uses this to auto-roll promptly. A wake that is never delivered costs
 * nothing beyond latency — the canonical timeout still resolves the attack.
 */
export interface DefenseNotification {
  type: "DefenseAvailable";
  notificationId: string;
  gameId: string;
  playerId: string;
  turnId: string;
  attackId: string;
  deadlineAt: number;
  causedBySourceOffset: string;
  decisionUrl: string;
}

export type PlayerActionNotification = TurnNotification | DefenseNotification;

/**
 * Both rulesets flow through one derivation. V1 simply never emits
 * `AttackDeclared`, so the defence branch is unreachable for it — cheaper and
 * less drift-prone than maintaining two notifiers that must agree on
 * `TurnAvailable` identities.
 */
type AnyGameEvent = GameEvent | GameEventV2;

interface OffsetEvent {
  event: AnyGameEvent;
  offset: string;
}

const eventSchema: JsonCodec<AnyGameEvent> = {
  encode: (event) => event,
  decode: (value) => value as AnyGameEvent,
};
const notificationSchema: JsonCodec<PlayerActionNotification> = {
  encode: (notification) => notification,
  decode: (value) => value as PlayerActionNotification,
};

export function deriveWakes(
  gameId: string,
  events: readonly OffsetEvent[],
): Map<string, PlayerActionNotification[]> {
  const byPlayer = new Map<string, PlayerActionNotification[]>();
  const push = (playerId: string, notification: PlayerActionNotification): void => {
    const list = byPlayer.get(playerId) ?? [];
    list.push(notification);
    byPlayer.set(playerId, list);
  };
  const decisionUrl = `/v1/games/${gameId}/decision`;

  for (const { event, offset } of events) {
    if (event.type === "GameStarted" && event.turnOrder[0]) {
      const playerId = event.turnOrder[0];
      push(playerId, {
        type: "TurnAvailable",
        notificationId: `turn:${gameId}:${playerId}:${event.round}`,
        gameId,
        playerId,
        turnId: buildTurnId(event.round, playerId),
        round: event.round,
        phase: "reinforce",
        causedBySourceOffset: offset,
        decisionUrl,
      });
    } else if (event.type === "TurnEnded") {
      push(event.nextPlayerId, {
        type: "TurnAvailable",
        notificationId: `turn:${gameId}:${event.nextPlayerId}:${event.round}`,
        gameId,
        playerId: event.nextPlayerId,
        turnId: buildTurnId(event.round, event.nextPlayerId),
        round: event.round,
        phase: "reinforce",
        causedBySourceOffset: offset,
        decisionUrl,
      });
    } else if (event.type === "AttackDeclared") {
      push(event.defenderId, {
        type: "DefenseAvailable",
        // `attackId` is already unique per game, so the identity is stable under
        // rebuild and cannot collide with a later attack on the same defender.
        notificationId: `defense:${gameId}:${event.defenderId}:${event.attackId}`,
        gameId,
        playerId: event.defenderId,
        turnId: event.turnId,
        attackId: event.attackId,
        deadlineAt: event.defenseDeadlineAt,
        causedBySourceOffset: offset,
        decisionUrl,
      });
    }
  }
  return byPlayer;
}

export async function catchUpTurns(protocol: StreamProtocolFactory, gameId: string): Promise<void> {
  await catchUpDerived({
    protocol,
    sourceStreamId: eventStreamId(gameId),
    sourceSchema: eventSchema,
    outputSchema: notificationSchema,
    derive: (messages) =>
      deriveWakes(
        gameId,
        messages.map((message) => ({ event: message.value, offset: message.offset })),
      ),
    streamIdFor: (playerId) => turnStreamId(gameId, playerId),
    producerIdFor: (playerId) => `risk-turns:${gameId}:${playerId}`,
  });
}

export async function readTurns(
  protocol: StreamProtocolFactory,
  gameId: string,
  playerId: string,
  options: { cursor?: string; waitMs?: number; signal?: AbortSignal } = {},
) {
  const result = await readDerived(
    protocol,
    turnStreamId(gameId, playerId),
    notificationSchema,
    options,
  );
  return { notifications: result.values, cursor: result.cursor, upToDate: result.upToDate };
}
