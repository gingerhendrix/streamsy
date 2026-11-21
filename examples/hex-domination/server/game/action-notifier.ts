/* oxlint-disable effecttsgo/async-function -- This module preserves a public Promise compatibility facade over protocol/runtime-owned application work. */
/** Replay-safe, self-sufficient per-player action-required streams. */
import { AppendResult, ZERO_OFFSET } from "@streamsy/core";
import { Effect, Option, Schema, Stream } from "effect";
import { PlayerActions, type GameStreams } from "./streams.ts";
import { readHistory } from "./canonical.ts";
import {
  AgentMessageSchema,
  type AgentMessage,
  type ActionRequired,
  type GameOver,
} from "./action-message.ts";

import { foldAggregate, buildTurnId } from "../../src/domain/aggregate.ts";
import type { PendingInteraction } from "../../src/domain/aggregate.ts";
import { decisionMode, legalActions } from "../../src/application/legal-actions.ts";
import type {
  DecisionMode,
  LegalAction as LegalActionType,
} from "../../src/application/legal-actions.ts";
import type { GameEvent as GameEventType } from "../../src/domain/events.ts";

export type ActionReason =
  | "turn-started"
  | "phase-changed"
  | "reinforcement-remaining"
  | "attack-resolved"
  | "occupation-required"
  | "defense-required";

export type { AgentMessage, ActionRequired, GameOver } from "./action-message.ts";
export { AgentMessageSchema } from "./action-message.ts";
interface OffsetEvent {
  event: GameEventType;
  offset: string;
}

/**
 * Why the server needs an action now. `ArmiesReinforced` is the one event whose
 * reason is not a function of the event type alone: while the pool still holds
 * armies the same phase is asking again (`reinforcement-remaining`), and the
 * placement that empties it moves the turn into `attack` (`phase-changed`).
 */
function reasonFor(
  event: GameEventType,
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
  legalMoves: LegalActionType[],
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

const encodeMessage = Schema.encodeSync(Schema.fromJsonString(AgentMessageSchema));

/** A duplicate producer sequence only identifies the sequence, not its payload. */
export const reconcilePlayerActions = Effect.fn("HexDomination.reconcilePlayerActions")(function* (
  gameId: string,
  playerId: string,
  desired: readonly AgentMessage[],
) {
  const stream = yield* PlayerActions;
  const params = { gameId, playerId };
  yield* stream.create(params);
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const { items, offset } = yield* stream.collect(params);
    for (const [seq, item] of items.entries()) {
      const expected = desired[seq];
      if (expected === undefined) return false;
      if (encodeMessage(item) !== encodeMessage(expected)) {
        return yield* Effect.die(
          new Error(
            `Player actions diverged at producer sequence ${seq}: a different durable payload won`,
          ),
        );
      }
    }
    let head = offset;
    let reload = false;
    for (let seq = items.length; seq < desired.length; seq += 1) {
      const item = desired[seq];
      if (item === undefined) return yield* Effect.die(new Error("Missing derived player action"));
      const result = yield* stream
        .append(params, [item], {
          producer: { producerId: "risk-actions", producerEpoch: 1, producerSeq: seq },
          expectedOffset: head,
        })
        .pipe(
          Effect.map((value): AppendResult | null => value),
          Effect.catchTag("OffsetMismatch", () => Effect.succeed(null)),
        );
      if (result === null || AppendResult.$is("Duplicate")(result)) {
        reload = true;
        break;
      }
      head = result.offset;
    }
    if (!reload) return true;
  }
  return yield* Effect.die(
    new Error("Player action append could not converge after bounded retries"),
  );
});

export function catchUpActions(streams: GameStreams, gameId: string): Promise<void> {
  return streams.runPromise(
    Effect.gen(function* () {
      for (let attempt = 0; attempt < 8; attempt += 1) {
        const { commands, head } = yield* readHistory(gameId);
        const desired = deriveActions(
          gameId,
          commands.flatMap(({ batch, offset }) => batch.events.map((event) => ({ event, offset }))),
        );
        let stale = false;
        for (const [playerId, messages] of desired) {
          if (!(yield* reconcilePlayerActions(gameId, playerId, messages))) {
            stale = true;
            break;
          }
        }
        if (!stale) return undefined;
        // A concurrent writer can derive a longer prefix from newer commands.
        // Re-read it, but never accept unexplained or mismatched durable rows.
        if ((yield* readHistory(gameId)).head === head) {
          return yield* Effect.die(new Error("Player actions extend beyond canonical history"));
        }
      }
      return yield* Effect.die(new Error("Player action replay could not converge"));
    }),
  );
}

export function readActions(
  streams: GameStreams,
  gameId: string,
  playerId: string,
  options: { cursor?: string; waitMs?: number; signal?: AbortSignal } = {},
) {
  const params = { gameId, playerId };
  const offset = options.cursor ?? ZERO_OFFSET;
  return streams.runPromise(
    Effect.gen(function* () {
      const stream = yield* PlayerActions;
      yield* stream.create(params);
      const immediate = yield* stream.read(params, { offset }).pipe(Stream.runHead);
      if (Option.isSome(immediate) && (immediate.value.items.length > 0 || !options.waitMs)) {
        return actionPage(immediate.value);
      }
      if (options.waitMs && options.waitMs > 0) {
        const next = yield* stream
          .follow(params, { offset })
          .pipe(Stream.runHead, Effect.timeoutOption(options.waitMs));
        if (Option.isSome(next) && Option.isSome(next.value)) return actionPage(next.value.value);
      }
      return { messages: [], nextOffset: offset, upToDate: true };
    }),
    { signal: options.signal },
  );
}

function actionPage(batch: {
  items: readonly AgentMessage[];
  nextOffset: string;
  upToDate: boolean;
}) {
  return { messages: [...batch.items], nextOffset: batch.nextOffset, upToDate: batch.upToDate };
}
