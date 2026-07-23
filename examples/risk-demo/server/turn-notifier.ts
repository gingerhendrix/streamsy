/** Risk bindings for replay-safe derived per-player turn streams. */
import type { StreamProtocolFactory } from "@streamsy/core";
import { catchUpDerived, readDerived } from "@streamsy/experimental/derived";
import type { JsonCodec } from "@streamsy/json";

import { buildTurnId } from "../src/aggregate.ts";
import type { GameEvent } from "../src/events.ts";
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

interface OffsetEvent {
  event: GameEvent;
  offset: string;
}

const eventSchema: JsonCodec<GameEvent> = {
  encode: (event) => event,
  decode: (value) => value as GameEvent,
};
const notificationSchema: JsonCodec<TurnNotification> = {
  encode: (notification) => notification,
  decode: (value) => value as TurnNotification,
};

export function deriveWakes(
  gameId: string,
  events: readonly OffsetEvent[],
): Map<string, TurnNotification[]> {
  const byPlayer = new Map<string, TurnNotification[]>();
  const push = (playerId: string, round: number, causedBySourceOffset: string): void => {
    const list = byPlayer.get(playerId) ?? [];
    list.push({
      type: "TurnAvailable",
      notificationId: `turn:${gameId}:${playerId}:${round}`,
      gameId,
      playerId,
      turnId: buildTurnId(round, playerId),
      round,
      phase: "reinforce",
      causedBySourceOffset,
      decisionUrl: `/v1/games/${gameId}/decision`,
    });
    byPlayer.set(playerId, list);
  };
  for (const { event, offset } of events) {
    if (event.type === "GameStarted" && event.turnOrder[0]) {
      push(event.turnOrder[0], event.round, offset);
    } else if (event.type === "TurnEnded") {
      push(event.nextPlayerId, event.round, offset);
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
