import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { JsonCodec } from "@streamsy/json";
import { CommandIdReuseError, createCommandLog, type CommandLogRecord } from "./command-log.ts";

type Event = { commandId: string; delta: number };
type Command = { id: string; delta: number };
const schema: JsonCodec<Event> = { encode: (value) => value, decode: (value) => value as Event };

function fixture(store = new Map<string, CommandLogRecord<Event, string>>()) {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  let decisions = 0;
  const log = () =>
    createCommandLog({
      protocol,
      streamId: "counter",
      eventSchema: schema,
      fold: (events: readonly Event[]) => events.reduce((sum, event) => sum + event.delta, 0),
      decide: (_state, command: Command) => {
        decisions += 1;
        return {
          status: "accepted" as const,
          events: [{ commandId: command.id, delta: command.delta }],
        };
      },
      commandIdOf: (command: Command) => command.id,
      eventCommandIdOf: (event: Event) => event.commandId,
      payloadOf: (command: Command) => ({ delta: command.delta }),
      store: {
        get: (id) => store.get(id) ?? null,
        put: (row) => void store.set(row.commandId, row),
      },
    });
  return { protocol, log, decisions: () => decisions };
}

describe("createCommandLog", () => {
  it("deduplicates before deciding and rejects payload reuse", async () => {
    const f = fixture();
    expect((await f.log().submit({ id: "one", delta: 1 })).status).toBe("accepted");
    expect((await f.log().submit({ id: "one", delta: 1 })).status).toBe("duplicate");
    expect(f.decisions()).toBe(1);
    await expect(f.log().submit({ id: "one", delta: 2 })).rejects.toBeInstanceOf(
      CommandIdReuseError,
    );
  });

  it("recovers an accepted command from canonical history after cache loss", async () => {
    const f = fixture();
    await f.log().submit({ id: "one", delta: 1 });
    const log = createCommandLog({
      protocol: f.protocol,
      streamId: "counter",
      eventSchema: schema,
      fold: (events: readonly Event[]) => events.length,
      decide: () => {
        throw new Error("must not decide");
      },
      commandIdOf: (command: Command) => command.id,
      eventCommandIdOf: (event: Event) => event.commandId,
      payloadOf: (command: Command) => ({ delta: command.delta }),
    });
    const result = await log.submit({ id: "one", delta: 1 });
    expect(result.status).toBe("duplicate");
    if (result.status === "rejected") throw new Error("unexpected rejection");
    expect(result.events).toEqual([{ commandId: "one", delta: 1 }]);
  });

  it("retries concurrent expected-offset races without losing either command", async () => {
    const f = fixture();
    const results = await Promise.all([
      f.log().submit({ id: "one", delta: 1 }),
      f.log().submit({ id: "two", delta: 2 }),
    ]);
    expect(results.every((result) => result.status === "accepted")).toBe(true);
    expect((await f.log().readAll()).events).toHaveLength(2);
  });
});
