import { ZERO_OFFSET, type StreamProtocolFactory } from "@streamsy/core";
import { createJsonProtocol, type JsonSchema } from "@streamsy/json";

const MAX_CAS_ATTEMPTS = 8;

export interface CommandLogRecord<Event, Error> {
  commandId: string;
  payloadHash: string;
  status: "accepted" | "rejected";
  sourceOffset?: string;
  events?: Event[];
  error?: Error;
}

export interface CommandLogStore<Event, Error> {
  get(commandId: string): CommandLogRecord<Event, Error> | null;
  put(record: CommandLogRecord<Event, Error>): void;
}

export type CommandDecision<Event, Error> =
  | { status: "accepted"; events: Event[] }
  | { status: "rejected"; error: Error };

export type CommandLogResult<Event, Error> =
  | {
      status: "accepted" | "duplicate";
      commandId: string;
      sourceStreamId: string;
      sourceOffset: string;
      events: Event[];
    }
  | { status: "rejected"; commandId: string; error: Error };

export interface CommandLogOptions<State, Event, Command, Error> {
  protocol: StreamProtocolFactory;
  streamId: string;
  eventSchema: JsonSchema<Event>;
  fold(events: readonly Event[]): State;
  decide(state: State, command: Command): CommandDecision<Event, Error>;
  commandIdOf(command: Command): string;
  eventCommandIdOf(event: Event): string;
  payloadOf(command: Command): unknown;
  store?: CommandLogStore<Event, Error>;
  maxAttempts?: number;
}

async function hashPayload(payload: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function createCommandLog<State, Event, Command, Error>(
  options: CommandLogOptions<State, Event, Command, Error>,
) {
  const json = createJsonProtocol(options.protocol, options.eventSchema);

  async function readAll() {
    const got = await json.get(options.streamId);
    if (got.status === "not-found") {
      return {
        events: [] as Event[],
        head: ZERO_OFFSET,
        byCommand: new Map<string, { events: Event[]; lastOffset: string }>(),
      };
    }
    if (got.status !== "ok") throw new Error(`cannot read command stream: ${got.status}`);
    const history = await got.stream.readAll();
    const byCommand = new Map<string, { events: Event[]; lastOffset: string }>();
    for (const message of history.messages) {
      const event = message.value;
      const commandId = options.eventCommandIdOf(event);
      const prior = byCommand.get(commandId);
      if (prior) {
        prior.events.push(event);
        prior.lastOffset = message.offset;
      } else {
        byCommand.set(commandId, { events: [event], lastOffset: message.offset });
      }
    }
    return { events: history.values, head: history.head, byCommand };
  }

  async function get(commandId: string) {
    const cached = options.store?.get(commandId);
    if (cached) return cached;
    const history = await readAll();
    const prior = history.byCommand.get(commandId);
    if (!prior) return null;
    return {
      commandId,
      payloadHash: "",
      status: "accepted" as const,
      sourceOffset: prior.lastOffset,
      events: prior.events,
    };
  }

  async function submit(command: Command): Promise<CommandLogResult<Event, Error>> {
    const commandId = options.commandIdOf(command);
    const payloadHash = await hashPayload(options.payloadOf(command));
    const cached = options.store?.get(commandId);
    if (cached) {
      if (cached.payloadHash !== payloadHash) {
        throw new CommandIdReuseError(commandId);
      }
      if (cached.status === "rejected") {
        return { status: "rejected", commandId, error: cached.error as Error };
      }
      return {
        status: "duplicate",
        commandId,
        sourceStreamId: options.streamId,
        sourceOffset: cached.sourceOffset ?? ZERO_OFFSET,
        events: cached.events ?? [],
      };
    }

    for (let attempt = 0; attempt < (options.maxAttempts ?? MAX_CAS_ATTEMPTS); attempt += 1) {
      const history = await readAll();
      const prior = history.byCommand.get(commandId);
      if (prior) {
        const sourceOffset = prior.lastOffset;
        options.store?.put({
          commandId,
          payloadHash,
          status: "accepted",
          sourceOffset,
          events: prior.events,
        });
        return {
          status: "duplicate",
          commandId,
          sourceStreamId: options.streamId,
          sourceOffset,
          events: prior.events,
        };
      }

      const state = options.fold(history.events);
      const decision = options.decide(state, command);
      if (decision.status === "rejected") {
        options.store?.put({ commandId, payloadHash, status: "rejected", error: decision.error });
        return { status: "rejected", commandId, error: decision.error };
      }
      if (decision.events.length === 0) {
        throw new Error(`accepted command ${commandId} produced no events`);
      }

      const stream = await json.getOrCreate(options.streamId);
      const appended = await stream.appendBatch(decision.events, { expectedOffset: history.head });
      if (appended.status === "appended") {
        options.store?.put({
          commandId,
          payloadHash,
          status: "accepted",
          sourceOffset: appended.offset,
          events: decision.events,
        });
        return {
          status: "accepted",
          commandId,
          sourceStreamId: options.streamId,
          sourceOffset: appended.offset,
          events: decision.events,
        };
      }
      if (appended.status === "conflict" && appended.conflictReason === "expected-offset") continue;
      throw new Error(`command append failed: ${appended.status}`);
    }
    throw new Error(`command ${commandId} could not commit after bounded retries`);
  }

  return { submit, get, readAll };
}

export class CommandIdReuseError extends Error {
  constructor(public readonly commandId: string) {
    super(`commandId ${commandId} reused with a different payload`);
    this.name = "CommandIdReuseError";
  }
}
