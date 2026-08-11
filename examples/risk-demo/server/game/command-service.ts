/**
 * Risk bindings for the generic Streamsy event-sourced command log.
 *
 * The log dedupes by `commandId`, folds canonical history, calls `decide`, and
 * appends with source-head CAS. The injected clock reading is a
 * command-service *input* recorded into `AttackDeclared` as a fact; nothing
 * downstream ever asks the current clock what should have happened.
 */
import {
  CommandIdReuseError,
  createCommandLog,
  readCommandHistory,
} from "../compat/command-log.ts";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";

import { foldAggregate } from "../../src/domain/aggregate.ts";
import type { Command } from "../../src/domain/commands.ts";
import { decide, type DecisionError } from "../../src/domain/decide.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { Rng } from "../../src/domain/rng.ts";
import { boardProjectionTxId } from "../../src/board/transaction.ts";
import type { CommandStore } from "../persistence/stores.ts";
import { ZERO_OFFSET, compareOffsets, type StreamProtocolFactory } from "@streamsy/core";

const eventSchema: JsonCodec<GameEvent> = {
  encode: (event) => event,
  decode: (value) => value as GameEvent,
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

export interface CommandServiceDeps {
  protocol: StreamProtocolFactory;
  commands: CommandStore;
  rng: Rng;
  now: () => number;
  /** Defence interrupt window; injectable so tests need not wait 15s. */
  defenseTimeoutMs?: number;
}

function commandLog(deps: CommandServiceDeps, sourceStreamId: string, gameId: string) {
  return createCommandLog({
    protocol: deps.protocol,
    streamId: sourceStreamId,
    eventSchema,
    fold: foldAggregate,
    // Every `decide` call sits *after* a fresh fold of canonical history, so the
    // dice a resolver rolls are consumed only once it has confirmed the attack it
    // names is still pending. A CAS-race loser refolds and is rejected instead.
    decide: (state, command: Command) =>
      decide(state, command, {
        rng: deps.rng,
        now: deps.now,
        defenseTimeoutMs: deps.defenseTimeoutMs,
      }),
    commandIdOf: (command: Command) => command.commandId,
    eventCommandIdOf: (event: GameEvent) => event.commandId,
    payloadOf: ({ commandId: _commandId, ...payload }: Command) => payload,
    store: {
      get: (commandId) => {
        const row = deps.commands.get(gameId, commandId);
        return row
          ? {
              ...row,
              events: row.events,
              error: row.error as DecisionError,
            }
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
    return { ...result, txid: boardProjectionTxId(result.commandId) };
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

export async function readCanonical(protocol: StreamProtocolFactory, sourceStreamId: string) {
  return readCommandHistory({
    protocol,
    streamId: sourceStreamId,
    eventSchema,
    eventCommandIdOf: (event: GameEvent) => event.commandId,
  });
}

/**
 * Read canonical history up to and including `throughOffset`.
 *
 * This is what lets the decision resource be *consistent with the board snapshot
 * it names*: the projection is caught up first, and the decision is then folded
 * from exactly the canonical prefix that projection has incorporated. A command
 * appended in between is simply not reflected yet — which is honest, and harmless,
 * because every command is revalidated against a fresh canonical fold anyway.
 */
export async function readCanonicalThrough(
  protocol: StreamProtocolFactory,
  sourceStreamId: string,
  throughOffset: string | null,
): Promise<{ events: GameEvent[]; head: string }> {
  const got = await createJsonProtocol(protocol, eventSchema).get(sourceStreamId);
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
