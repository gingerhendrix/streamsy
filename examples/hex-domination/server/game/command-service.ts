/* oxlint-disable effecttsgo/async-function -- The Web API calls this Promise facade over one host-owned runtime. */
import { AppendResult, compare } from "@streamsy/core";
import { Array, Effect } from "effect";
import { sha256Hex } from "../capabilities.ts";
import type { CommandStore } from "../persistence/stores.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import type { Command } from "../../src/domain/commands.ts";
import { decide, type DecisionError } from "../../src/domain/decide.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { Rng } from "../../src/domain/rng.ts";
import { boardProjectionTxId } from "../../src/board/transaction.ts";
import { readHistory } from "./canonical.ts";
import { GameEvents, type GameStreams } from "./streams.ts";

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
  streams: GameStreams;
  commands: CommandStore;
  rng: Rng;
  now: () => number;
  defenseTimeoutMs?: number;
}

function gameIdFromStream(sourceStreamId: string): string {
  const match = /^games\/([^/]+)\/events$/.exec(sourceStreamId);
  if (!match?.[1]) throw new Error(`Invalid canonical stream id: ${sourceStreamId}`);
  return match[1];
}

export async function submitCommand(
  deps: CommandServiceDeps,
  sourceStreamId: string,
  command: Command,
): Promise<SubmitResult> {
  const gameId = gameIdFromStream(sourceStreamId);
  if ("gameId" in command && command.gameId !== gameId)
    throw new Error("Command targets a different game");
  const params = { gameId };
  const { commandId, ...payload } = command;
  const payloadHash = await sha256Hex(JSON.stringify(payload));
  const rejected = (error: DecisionError): SubmitResult => ({
    status: "rejected",
    commandId,
    error,
  });
  const reused = () =>
    rejected({ code: "COMMAND_ID_REUSED", message: "commandId reused with a different payload." });
  const ack = (
    status: "accepted" | "duplicate",
    sourceOffset: string,
    events: readonly GameEvent[],
  ): SubmitResult => ({
    status,
    sourceStreamId,
    sourceOffset,
    commandId,
    events: [...events],
    txid: boardProjectionTxId(commandId),
  });

  const recordAccepted = (sourceOffset: string, events: readonly GameEvent[]) => {
    deps.commands.put({
      gameId,
      commandId,
      payloadHash,
      status: "accepted",
      sourceOffset,
      events: [...events],
      createdAt: deps.now(),
    });
  };

  return deps.streams.runPromise(
    Effect.gen(function* () {
      const stream = yield* GameEvents;
      yield* stream.create(params);
      for (let attempt = 0; attempt < 8; attempt += 1) {
        const history = yield* readHistory(gameId);
        const prior = history.commands.find(({ batch }) => batch.events[0].commandId === commandId);
        const cached = deps.commands.get(gameId, commandId);
        if (prior) {
          // The canonical receipt is authoritative even if the process died before
          // saving the metadata row. A producer acknowledgement alone cannot prove this.
          if (prior.batch.payloadHash !== payloadHash) return reused();
          if (cached?.status !== "accepted") recordAccepted(prior.offset, prior.batch.events);
          return ack("duplicate", prior.offset, prior.batch.events);
        }
        if (cached) {
          if (cached.payloadHash !== payloadHash) return reused();
          if (cached.status === "rejected" && cached.error) return rejected(cached.error);
        }
        const state = foldAggregate(history.commands.flatMap(({ batch }) => batch.events));
        const decision = decide(state, command, deps);
        if (decision.status === "rejected") {
          deps.commands.put({
            gameId,
            commandId,
            payloadHash,
            status: "rejected",
            error: decision.error,
            createdAt: deps.now(),
          });
          return rejected(decision.error);
        }
        const events = decision.events;
        if (!Array.isReadonlyArrayNonEmpty(events))
          return yield* Effect.die(new Error(`Command ${commandId} emitted no events`));
        const result = yield* stream
          .append(params, [{ payloadHash, events }], { expectedOffset: history.head })
          .pipe(
            Effect.map((value): AppendResult | null => value),
            Effect.catchTag("OffsetMismatch", () => Effect.succeed(null)),
          );
        if (result === null) continue;
        if (!AppendResult.$is("Appended")(result))
          return yield* Effect.die(new Error("Unconditional producer replay on command stream"));
        recordAccepted(result.offset, events);
        return ack("accepted", result.offset, events);
      }
      return yield* Effect.die(
        new Error(`Command ${commandId} could not commit after bounded retries`),
      );
    }),
  );
}

export function readCanonical(streams: GameStreams, sourceStreamId: string) {
  return readCanonicalThrough(streams, sourceStreamId);
}

export async function readCanonicalThrough(
  streams: GameStreams,
  sourceStreamId: string,
  throughOffset?: string | null,
): Promise<{ events: GameEvent[]; head: string }> {
  const history = await streams.runPromise(readHistory(gameIdFromStream(sourceStreamId)));
  const events = history.commands.flatMap(({ batch, offset }) =>
    throughOffset === undefined || (throughOffset !== null && compare(offset, throughOffset) <= 0)
      ? batch.events
      : [],
  );
  return { events, head: history.head };
}
