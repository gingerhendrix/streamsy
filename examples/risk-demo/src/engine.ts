/**
 * Command driver: fold history, deduplicate, decide, append, acknowledge.
 *
 * This is the authoritative decision loop from `demo-concept.md`, minus the
 * durable stream (Batch 2/3): it folds canonical history to the head, checks the
 * `commandId` idempotency key *before* consuming randomness, validates against
 * that exact head, and returns an acknowledgement naming the resulting source
 * offset. Offsets here are the positional index of an event in the log.
 */

import type { Command } from "./commands.ts";
import type { GameEvent } from "./events.ts";
import type { DecisionError } from "./decide.ts";
import { decide } from "./decide.ts";
import { foldAggregate } from "./aggregate.ts";
import { createSeededRng, type Rng } from "./rng.ts";

export interface CommandAck {
  status: "accepted" | "duplicate";
  commandId: string;
  turnId?: string;
  /** Positional index (as a string) of the command's last appended event. */
  sourceOffset: string;
  events: GameEvent[];
}

export interface CommandRejection {
  status: "rejected";
  commandId: string;
  error: DecisionError;
}

export type CommandOutcome = CommandAck | CommandRejection;

export interface ApplyResult {
  /** The full event log after applying the command (unchanged on rejection). */
  events: GameEvent[];
  outcome: CommandOutcome;
}

function turnIdOf(command: Command): string | undefined {
  return "turnId" in command ? command.turnId : undefined;
}

/**
 * Apply a single command to an immutable event log. Returns a new log plus a
 * canonical outcome. A previously accepted `commandId` short-circuits to its
 * original events and offset without re-deciding or rolling dice.
 */
export function applyCommand(
  events: readonly GameEvent[],
  command: Command,
  rng: Rng,
): ApplyResult {
  const state = foldAggregate(events);

  const prior = state.commandIndex[command.commandId];
  if (prior) {
    return {
      events: events.slice(),
      outcome: {
        status: "duplicate",
        commandId: command.commandId,
        turnId: turnIdOf(command),
        sourceOffset: String(prior.lastOffset),
        events: prior.events.slice(),
      },
    };
  }

  const decision = decide(state, command, rng);
  if (decision.status === "rejected") {
    return {
      events: events.slice(),
      outcome: {
        status: "rejected",
        commandId: command.commandId,
        error: decision.error,
      },
    };
  }

  const nextEvents = events.concat(decision.events);
  return {
    events: nextEvents,
    outcome: {
      status: "accepted",
      commandId: command.commandId,
      turnId: turnIdOf(command),
      sourceOffset: String(nextEvents.length - 1),
      events: decision.events,
    },
  };
}

/**
 * Small mutable convenience wrapper around `applyCommand` for tests, scripts and
 * demos. The canonical log stays available via {@link RiskGame.log}.
 */
export class RiskGame {
  private events: GameEvent[] = [];

  constructor(private readonly rng: Rng = createSeededRng(1)) {}

  get log(): readonly GameEvent[] {
    return this.events;
  }

  submit(command: Command): CommandOutcome {
    const result = applyCommand(this.events, command, this.rng);
    this.events = result.events;
    return result.outcome;
  }

  state() {
    return foldAggregate(this.events);
  }
}
