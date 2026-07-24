/** Risk bindings for the generic Streamsy event-sourced command log. */
import {
  CommandIdReuseError,
  createCommandLog,
  readCommandHistory,
} from "@streamsy/experimental/command";
import type { JsonCodec } from "@streamsy/json";

import { foldAggregate } from "../../src/domain/aggregate.ts";
import type { Command } from "../../src/domain/commands.ts";
import { decide, type DecisionError } from "../../src/domain/decide.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { Rng } from "../../src/domain/rng.ts";
import { boardProjectionTxId } from "../../src/board/transaction.ts";
import type { CommandStore } from "../persistence/stores.ts";
import type { StreamProtocolFactory } from "@streamsy/core";

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
      get: (commandId) => deps.commands.get(gameId, commandId),
      put: (record) => deps.commands.put({ ...record, gameId, createdAt: deps.now() }),
    },
  });
}

export async function submitCommand(
  deps: CommandServiceDeps,
  sourceStreamId: string,
  command: Command,
): Promise<SubmitResult> {
  const gameId = "gameId" in command ? command.gameId : sourceStreamId.split("/")[1]!;
  try {
    const result = await commandLog(deps, sourceStreamId, gameId).submit(command);
    if (result.status === "rejected") return result;
    return {
      ...result,
      txid: boardProjectionTxId(result.commandId, result.sourceOffset),
    };
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
