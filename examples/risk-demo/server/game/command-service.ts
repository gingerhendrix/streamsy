/**
 * Risk bindings for the generic Streamsy event-sourced command log.
 *
 * The log itself is ruleset-agnostic: it dedupes by `commandId`, folds canonical
 * history, calls `decide`, and appends with source-head CAS. What differs between
 * `risk-demo-v1` and `risk-demo-v2` is only which (fold, decide) pair it is given,
 * so the two rulesets share every idempotency and race guarantee rather than
 * reimplementing them.
 *
 * The v2 pair additionally consumes an injected clock. That clock reading is a
 * command-service *input* recorded into `AttackDeclared` as a fact; nothing
 * downstream ever asks the current clock what should have happened.
 */
import {
  CommandIdReuseError,
  createCommandLog,
  readCommandHistory,
} from "@streamsy/experimental/command";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";

import { foldAggregate } from "../../src/domain/aggregate.ts";
import { foldAggregateV2 } from "../../src/domain/aggregate-v2.ts";
import type { Command } from "../../src/domain/commands.ts";
import type { CommandV2 } from "../../src/domain/commands-v2.ts";
import { decide, type DecisionError } from "../../src/domain/decide.ts";
import { decideV2, type DecisionErrorV2 } from "../../src/domain/decide-v2.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { GameEventV2 } from "../../src/domain/events-v2.ts";
import { RULESET_V2 } from "../../src/domain/map-v2.ts";
import type { Rng } from "../../src/domain/rng.ts";
import { boardProjectionTxId } from "../../src/board/transaction.ts";
import type { CommandStore } from "../persistence/stores.ts";
import { ZERO_OFFSET, compareOffsets, type StreamProtocolFactory } from "@streamsy/core";

const eventSchema: JsonCodec<GameEvent> = {
  encode: (event) => event,
  decode: (value) => value as GameEvent,
};
const eventSchemaV2: JsonCodec<GameEventV2> = {
  encode: (event) => event,
  decode: (value) => value as GameEventV2,
};

export type SubmitResult =
  | {
      status: "accepted" | "duplicate";
      commandId: string;
      sourceStreamId: string;
      sourceOffset: string;
      txid: string;
      events: GameEvent[];
    }
  | { status: "rejected"; commandId: string; error: DecisionError };

export type SubmitResultV2 =
  | {
      status: "accepted" | "duplicate";
      commandId: string;
      sourceStreamId: string;
      sourceOffset: string;
      txid: string;
      events: GameEventV2[];
    }
  | { status: "rejected"; commandId: string; error: DecisionErrorV2 };

export interface CommandServiceDeps {
  protocol: StreamProtocolFactory;
  commands: CommandStore;
  rng: Rng;
  now: () => number;
  /** Defence interrupt window for v2; injectable so tests need not wait 15s. */
  defenseTimeoutMs?: number;
}

function commandLog(deps: CommandServiceDeps, sourceStreamId: string, gameId: string) {
  return createCommandLog({
    protocol: deps.protocol,
    streamId: sourceStreamId,
    eventSchema,
    fold: foldAggregate,
    decide: (state, command: Command) => decide(state, command, deps.rng),
    commandIdOf: (command: Command) => command.commandId,
    eventCommandIdOf: (event: GameEvent) => event.commandId,
    payloadOf: ({ commandId: _commandId, ...payload }: Command) => payload,
    store: {
      get: (commandId) => {
        const row = deps.commands.get(gameId, commandId);
        return row
          ? { ...row, events: row.events as GameEvent[], error: row.error as DecisionError }
          : null;
      },
      put: (record) => deps.commands.put({ ...record, gameId, createdAt: deps.now() }),
    },
  });
}

function commandLogV2(deps: CommandServiceDeps, sourceStreamId: string, gameId: string) {
  return createCommandLog({
    protocol: deps.protocol,
    streamId: sourceStreamId,
    eventSchema: eventSchemaV2,
    fold: foldAggregateV2,
    // Every `decide` call sits *after* a fresh fold of canonical history, so the
    // dice a resolver rolls are consumed only once it has confirmed the attack it
    // names is still pending. A CAS-race loser refolds and is rejected instead.
    decide: (state, command: CommandV2) =>
      decideV2(state, command, {
        rng: deps.rng,
        now: deps.now,
        defenseTimeoutMs: deps.defenseTimeoutMs,
      }),
    commandIdOf: (command: CommandV2) => command.commandId,
    eventCommandIdOf: (event: GameEventV2) => event.commandId,
    payloadOf: ({ commandId: _commandId, ...payload }: CommandV2) => payload,
    store: {
      get: (commandId) => {
        const row = deps.commands.get(gameId, commandId);
        return row
          ? { ...row, events: row.events as GameEventV2[], error: row.error as DecisionErrorV2 }
          : null;
      },
      put: (record) => deps.commands.put({ ...record, gameId, createdAt: deps.now() }),
    },
  });
}

function gameIdFor(sourceStreamId: string, command: { gameId?: string }): string {
  return command.gameId ?? sourceStreamId.split("/")[1]!;
}

export async function submitCommand(
  deps: CommandServiceDeps,
  sourceStreamId: string,
  command: Command,
): Promise<SubmitResult> {
  const gameId = gameIdFor(sourceStreamId, command as { gameId?: string });
  try {
    const result = await commandLog(deps, sourceStreamId, gameId).submit(command);
    if (result.status === "rejected") return result;
    return { ...result, txid: boardProjectionTxId(result.commandId, result.sourceOffset) };
  } catch (error) {
    if (error instanceof CommandIdReuseError) {
      return {
        status: "rejected",
        commandId: command.commandId,
        error: { code: "COMMAND_ID_REUSED", message: "commandId reused with a different payload." },
      };
    }
    throw error;
  }
}

export async function submitCommandV2(
  deps: CommandServiceDeps,
  sourceStreamId: string,
  command: CommandV2,
): Promise<SubmitResultV2> {
  const gameId = gameIdFor(sourceStreamId, command as { gameId?: string });
  try {
    const result = await commandLogV2(deps, sourceStreamId, gameId).submit(command);
    if (result.status === "rejected") return result;
    return { ...result, txid: boardProjectionTxId(result.commandId, result.sourceOffset) };
  } catch (error) {
    if (error instanceof CommandIdReuseError) {
      return {
        status: "rejected",
        commandId: command.commandId,
        error: { code: "COMMAND_ID_REUSED", message: "commandId reused with a different payload." },
      };
    }
    throw error;
  }
}

export function isRulesetV2(ruleset: string | undefined): boolean {
  return ruleset === RULESET_V2;
}

/**
 * Raised when the durable `GameRow.ruleset` routing hint disagrees with the
 * canonical `GameCreated.ruleset`.
 *
 * The row exists only so the service can pick a fold/decide pair without first
 * reading the stream; canonical history is the authority. A disagreement means
 * commands could be validated by the wrong ruleset, so it fails loudly rather
 * than folding a v2 stream with the v1 reducer into a plausible board.
 */
export class RulesetMismatchError extends Error {
  constructor(
    readonly gameId: string,
    readonly routed: string,
    readonly canonical: string,
  ) {
    super(`game ${gameId} is routed as "${routed}" but canonical history says "${canonical}"`);
    this.name = "RulesetMismatchError";
  }
}

/** The ruleset canonical history declares, read from `GameCreated` itself. */
export function canonicalRulesetOf(
  events: ReadonlyArray<GameEvent | GameEventV2>,
): string | undefined {
  const created = events.find((event) => event.type === "GameCreated");
  return created?.ruleset;
}

/** Cross-check a routing decision against the ruleset recorded in `GameCreated`. */
export function assertRulesetMatches(
  gameId: string,
  routed: string,
  events: ReadonlyArray<GameEvent | GameEventV2>,
): void {
  const canonical = canonicalRulesetOf(events);
  if (canonical !== undefined && canonical !== routed) {
    throw new RulesetMismatchError(gameId, routed, canonical);
  }
}

export async function readCanonical(protocol: StreamProtocolFactory, sourceStreamId: string) {
  return readCommandHistory({
    protocol,
    streamId: sourceStreamId,
    eventSchema,
    eventCommandIdOf: (event: GameEvent) => event.commandId,
  });
}

export async function readCanonicalV2(protocol: StreamProtocolFactory, sourceStreamId: string) {
  return readCommandHistory({
    protocol,
    streamId: sourceStreamId,
    eventSchema: eventSchemaV2,
    eventCommandIdOf: (event: GameEventV2) => event.commandId,
  });
}

/**
 * Read canonical v2 history up to and including `throughOffset`.
 *
 * This is what lets the decision resource be *consistent with the board snapshot
 * it names*: the projection is caught up first, and the decision is then folded
 * from exactly the canonical prefix that projection has incorporated. A command
 * appended in between is simply not reflected yet — which is honest, and harmless,
 * because every command is revalidated against a fresh canonical fold anyway.
 */
export async function readCanonicalV2Through(
  protocol: StreamProtocolFactory,
  sourceStreamId: string,
  throughOffset: string | null,
): Promise<{ events: GameEventV2[]; head: string }> {
  const got = await createJsonProtocol(protocol, eventSchemaV2).get(sourceStreamId);
  if (got.status === "not-found") return { events: [], head: ZERO_OFFSET };
  if (got.status !== "ok") throw new Error(`cannot read command stream: ${got.status}`);
  const history = await got.stream.readAll();
  const events =
    throughOffset === null
      ? []
      : history.messages
          .filter((message) => compareOffsets(message.offset, throughOffset) <= 0)
          .map((message) => message.value);
  return { events, head: history.head };
}
