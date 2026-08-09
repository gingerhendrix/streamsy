/** Replay-safe, self-sufficient per-player action-required streams. */
import type { StreamProtocolFactory } from "@streamsy/core";
import { catchUpDerived, readDerived } from "../compat/derived-streams.ts";
import type { JsonCodec } from "@streamsy/json";

import { foldAggregate, buildTurnId } from "../../src/domain/aggregate.ts";
import type { PendingInteraction, ReinforcementState } from "../../src/domain/aggregate.ts";
import { decisionMode, legalActions } from "../../src/application/legal-actions.ts";
import type { DecisionMode, LegalAction } from "../../src/application/legal-actions.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import { actionStreamId, eventStreamId } from "./names.ts";

export type ActionReason =
  | "turn-started"
  | "phase-changed"
  | "reinforcement-remaining"
  | "attack-resolved"
  | "occupation-required"
  | "defense-required";

export interface ActionRequired {
  type: "ActionRequired";
  messageId: string;
  seq: number;
  gameId: string;
  playerId: string;
  reason: ActionReason;
  turn: {
    id: string;
    round: number;
    phase: "reinforce" | "attack" | "fortify";
    activePlayerId: string;
    reinforcement: ReinforcementState;
  };
  mode: Exclude<DecisionMode, "waiting" | "finished">;
  pendingInteraction: PendingInteraction | null;
  legalMoves: LegalAction[];
  board: {
    territories: Array<{ id: string; ownerId: string | null; armies: number }>;
    players: Array<{ id: string; eliminated: boolean }>;
  };
  since: { fromEventOffset: string | null; events: GameEvent[] };
  eventOffset: string;
}

export interface GameOver {
  type: "GameOver";
  messageId: string;
  seq: number;
  gameId: string;
  playerId: string;
  winner: { id: string; name: string };
  since: { fromEventOffset: string | null; events: GameEvent[] };
  eventOffset: string;
}

export type AgentMessage = ActionRequired | GameOver;
interface OffsetEvent {
  event: GameEvent;
  offset: string;
}

const eventSchema: JsonCodec<GameEvent> = {
  encode: (event) => event,
  decode: (value) => value as GameEvent,
};
const messageSchema: JsonCodec<AgentMessage> = {
  encode: (message) => message,
  decode: (value) => value as AgentMessage,
};

/**
 * Why the server needs an action now. `ArmiesReinforced` is the one event whose
 * reason is not a function of the event type alone: while the pool still holds
 * armies the same phase is asking again (`reinforcement-remaining`), and the
 * placement that empties it moves the turn into `attack` (`phase-changed`).
 */
function reasonFor(
  event: GameEvent,
  pending: PendingInteraction | undefined,
  reinforcementRemaining: number,
): ActionReason {
  if (event.type === "GameStarted" || event.type === "TurnEnded") return "turn-started";
  if (pending?.type === "occupation") return "occupation-required";
  if (pending?.type === "defense") return "defense-required";
  if (event.type === "ArmiesReinforced")
    return reinforcementRemaining > 0 ? "reinforcement-remaining" : "phase-changed";
  if (event.type === "AttackResolved" || event.type === "TerritoryOccupied")
    return "attack-resolved";
  return "phase-changed";
}

function signature(
  mode: DecisionMode,
  turnId: string,
  phase: string | undefined,
  pending: PendingInteraction | undefined,
  legalMoves: LegalAction[],
): string {
  return JSON.stringify([
    mode,
    turnId,
    phase,
    pending?.type === "defense" || pending?.type === "occupation" ? pending.attackId : null,
    legalMoves,
  ]);
}

export function deriveActions(
  gameId: string,
  source: readonly OffsetEvent[],
): Map<string, AgentMessage[]> {
  const output = new Map<string, AgentMessage[]>();
  const lastSignature = new Map<string, string>();
  const lastEventIndex = new Map<string, number>();

  for (let index = 0; index < source.length; index += 1) {
    const current = source[index]!;
    const prefix = source.slice(0, index + 1);
    const state = foldAggregate(prefix.map(({ event }) => event));

    if (current.event.type === "GameWon") {
      const won = current.event;
      const winner = state.players.find((player) => player.id === won.playerId)!;
      for (const player of state.players) {
        const seq = (output.get(player.id)?.length ?? 0) + 1;
        const from = lastEventIndex.get(player.id);
        const events = prefix.slice(from === undefined ? 0 : from + 1).map(({ event }) => event);
        const message: GameOver = {
          type: "GameOver",
          messageId: `act:${gameId}:${player.id}:${seq}`,
          seq,
          gameId,
          playerId: player.id,
          winner: { id: winner.id, name: winner.name },
          since: {
            fromEventOffset: from === undefined ? null : source[from]!.offset,
            events,
          },
          eventOffset: current.offset,
        };
        output.set(player.id, [...(output.get(player.id) ?? []), message]);
        lastEventIndex.set(player.id, index);
      }
      continue;
    }

    if (state.status !== "playing" || !state.activePlayerId || !state.phase) continue;
    const turnId = buildTurnId(state.round, state.activePlayerId);
    for (const player of state.players) {
      const legalMoves = legalActions(state, player.id);
      if (legalMoves.length === 0) {
        // Becoming blocked is itself a signature transition. Forget the last
        // actionable signature so returning from defence resolution emits even
        // when the available choices happen to be byte-identical to pre-attack.
        lastSignature.delete(player.id);
        continue;
      }
      const mode = decisionMode(state, player.id);
      if (mode !== "active-turn" && mode !== "defense") continue;
      const sig = signature(mode, turnId, state.phase, state.pendingInteraction, legalMoves);
      if (sig === lastSignature.get(player.id)) continue;

      const seq = (output.get(player.id)?.length ?? 0) + 1;
      const from = lastEventIndex.get(player.id);
      const pending =
        state.pendingInteraction?.type === "defense" &&
        state.players.find((candidate) => candidate.id === player.id)?.controller ===
          "external-agent"
          ? null
          : (state.pendingInteraction ?? null);
      const message: ActionRequired = {
        type: "ActionRequired",
        messageId: `act:${gameId}:${player.id}:${seq}`,
        seq,
        gameId,
        playerId: player.id,
        reason: reasonFor(current.event, state.pendingInteraction, state.reinforcement.remaining),
        turn: {
          id: turnId,
          round: state.round,
          phase: state.phase,
          activePlayerId: state.activePlayerId,
          reinforcement: state.reinforcement,
        },
        mode,
        pendingInteraction: pending,
        legalMoves,
        board: {
          territories: Object.values(state.territories)
            .map(({ id, ownerId, armies }) => ({ id, ownerId: ownerId ?? null, armies }))
            .toSorted((a, b) => a.id.localeCompare(b.id)),
          players: state.players
            .map(({ id, eliminated }) => ({ id, eliminated }))
            .toSorted((a, b) => a.id.localeCompare(b.id)),
        },
        since: {
          fromEventOffset: from === undefined ? null : source[from]!.offset,
          events: prefix.slice(from === undefined ? 0 : from + 1).map(({ event }) => event),
        },
        eventOffset: current.offset,
      };
      output.set(player.id, [...(output.get(player.id) ?? []), message]);
      lastSignature.set(player.id, sig);
      lastEventIndex.set(player.id, index);
    }
  }
  return output;
}

export async function catchUpActions(
  protocol: StreamProtocolFactory,
  gameId: string,
): Promise<void> {
  await catchUpDerived({
    protocol,
    sourceStreamId: eventStreamId(gameId),
    sourceSchema: eventSchema,
    outputSchema: messageSchema,
    derive: (messages) =>
      deriveActions(
        gameId,
        messages.map(({ value, offset }) => ({ event: value, offset })),
      ),
    streamIdFor: (playerId) => actionStreamId(gameId, playerId),
    producerIdFor: (playerId) => `risk-actions:${gameId}:${playerId}`,
  });
}

export async function readActions(
  protocol: StreamProtocolFactory,
  gameId: string,
  playerId: string,
  options: { cursor?: string; waitMs?: number; signal?: AbortSignal } = {},
) {
  const result = await readDerived(
    protocol,
    actionStreamId(gameId, playerId),
    messageSchema,
    options,
  );
  return { messages: result.values, nextOffset: result.cursor, upToDate: result.upToDate };
}
