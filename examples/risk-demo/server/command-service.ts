/**
 * Authoritative command decision loop over a durable Streamsy event stream.
 *
 * For every command: fold canonical history to its exact head, deduplicate the
 * `commandId` before consuming randomness, validate with the Batch 1 kernel,
 * resolve dice once, then CAS-append the accepted event batch at the folded head
 * (`expectedOffset`). On an expected-offset conflict it refolds/revalidates and
 * retries, bounded. Idempotency is anchored in the canonical stream (each event
 * carries its `commandId`), so a retry returns the original ack even if the
 * command-log row was lost to a crash.
 */

import { ZERO_OFFSET } from "@streamsy/core";
import type { ProtocolStream, StreamProtocolFactory } from "@streamsy/core";

import type { Command } from "../src/commands.ts";
import type { GameEvent } from "../src/events.ts";
import { foldAggregate } from "../src/aggregate.ts";
import { decide } from "../src/decide.ts";
import type { Rng } from "../src/rng.ts";
import { sha256Hex } from "./capabilities.ts";
import type { CommandStore } from "./stores.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const MAX_CAS_ATTEMPTS = 8;

export interface CommandAck {
  status: "accepted" | "duplicate";
  commandId: string;
  sourceStreamId: string;
  sourceOffset: string;
  events: GameEvent[];
}

export interface CommandRejection {
  status: "rejected";
  commandId: string;
  error: { code: string; message: string; currentTurnId?: string };
}

export type SubmitResult = CommandAck | CommandRejection;

export interface CommandServiceDeps {
  protocol: StreamProtocolFactory;
  commands: CommandStore;
  rng: Rng;
  now: () => number;
}

/** A stable fingerprint of a command's intent (excluding its idempotency key). */
async function payloadHash(command: Command): Promise<string> {
  const { commandId: _ignored, ...rest } = command;
  return sha256Hex(JSON.stringify(rest));
}

interface CanonicalHistory {
  events: GameEvent[];
  head: string;
  /** commandId -> the events it appended and the offset of its last event. */
  byCommand: Map<string, { events: GameEvent[]; lastOffset: string }>;
}

async function readCanonical(
  protocol: StreamProtocolFactory,
  sourceStreamId: string,
): Promise<CanonicalHistory> {
  const got = await protocol.get(sourceStreamId);
  if (got.status !== "ok") return { events: [], head: ZERO_OFFSET, byCommand: new Map() };

  const events: GameEvent[] = [];
  const byCommand = new Map<string, { events: GameEvent[]; lastOffset: string }>();
  let head = ZERO_OFFSET;
  let offset: string | undefined;
  for (;;) {
    const read = await got.stream.read({ offset });
    if (read.status !== "ok") break;
    for (const message of read.messages) {
      const event = JSON.parse(decoder.decode(message.data)) as GameEvent;
      events.push(event);
      head = message.offset;
      const existing = byCommand.get(event.commandId);
      if (existing) {
        existing.events.push(event);
        existing.lastOffset = message.offset;
      } else {
        byCommand.set(event.commandId, { events: [event], lastOffset: message.offset });
      }
    }
    if (read.upToDate || read.messages.length === 0) break;
    offset = read.nextOffset;
  }
  return { events, head, byCommand };
}

async function requireStream(
  protocol: StreamProtocolFactory,
  streamId: string,
): Promise<ProtocolStream> {
  const got = await protocol.get(streamId);
  if (got.status === "ok") return got.stream;
  const created = await protocol.create(streamId, { contentType: "application/json" });
  if (created.status === "created" || created.status === "exists") return created.stream;
  throw new Error(`cannot open event stream ${streamId}: ${created.status}`);
}

function ackFromRow(
  commandId: string,
  sourceStreamId: string,
  row: { sourceOffset?: string; events?: GameEvent[] },
): CommandAck {
  return {
    status: "duplicate",
    commandId,
    sourceStreamId,
    sourceOffset: row.sourceOffset ?? ZERO_OFFSET,
    events: row.events ?? [],
  };
}

/**
 * Decide and durably commit one command against `sourceStreamId`. The stream is
 * created on demand (e.g. the first `create-game`).
 */
export async function submitCommand(
  deps: CommandServiceDeps,
  sourceStreamId: string,
  command: Command,
): Promise<SubmitResult> {
  const gameId = "gameId" in command ? command.gameId : sourceStreamId;
  const hash = await payloadHash(command);

  // Fast idempotency/recovery path from the durable command log.
  const priorRow = deps.commands.get(gameId, command.commandId);
  if (priorRow) {
    if (priorRow.payloadHash !== hash) {
      return {
        status: "rejected",
        commandId: command.commandId,
        error: { code: "COMMAND_ID_REUSED", message: "commandId reused with a different payload." },
      };
    }
    if (priorRow.status === "accepted")
      return ackFromRow(command.commandId, sourceStreamId, priorRow);
    return {
      status: "rejected",
      commandId: command.commandId,
      error: priorRow.error ?? { code: "ILLEGAL_ACTION", message: "rejected" },
    };
  }

  for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt += 1) {
    const history = await readCanonical(deps.protocol, sourceStreamId);

    // Idempotency anchored in the canonical stream (survives command-log loss).
    const prior = history.byCommand.get(command.commandId);
    if (prior) {
      deps.commands.put({
        gameId,
        commandId: command.commandId,
        payloadHash: hash,
        status: "accepted",
        sourceOffset: prior.lastOffset,
        events: prior.events,
        createdAt: deps.now(),
      });
      return {
        status: "duplicate",
        commandId: command.commandId,
        sourceStreamId,
        sourceOffset: prior.lastOffset,
        events: prior.events,
      };
    }

    const state = foldAggregate(history.events);
    const decision = decide(state, command, deps.rng);
    if (decision.status === "rejected") {
      deps.commands.put({
        gameId,
        commandId: command.commandId,
        payloadHash: hash,
        status: "rejected",
        error: decision.error,
        createdAt: deps.now(),
      });
      return { status: "rejected", commandId: command.commandId, error: decision.error };
    }

    const stream = await requireStream(deps.protocol, sourceStreamId);
    const result = await stream.append({
      data: encoder.encode(JSON.stringify(decision.events)),
      contentType: "application/json",
      expectedOffset: history.head,
    });

    if (result.status === "appended") {
      deps.commands.put({
        gameId,
        commandId: command.commandId,
        payloadHash: hash,
        status: "accepted",
        sourceOffset: result.offset,
        events: decision.events,
        createdAt: deps.now(),
      });
      return {
        status: "accepted",
        commandId: command.commandId,
        sourceStreamId,
        sourceOffset: result.offset,
        events: decision.events,
      };
    }
    if (result.status === "conflict" && result.conflictReason === "expected-offset") {
      continue; // lost the CAS race; refold against the new head and revalidate
    }
    throw new Error(`event append failed for ${sourceStreamId}: ${result.status}`);
  }

  throw new Error(
    `command ${command.commandId} could not commit after ${MAX_CAS_ATTEMPTS} attempts`,
  );
}

export { readCanonical };
