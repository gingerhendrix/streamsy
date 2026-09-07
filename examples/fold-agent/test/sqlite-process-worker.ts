/* oxlint-disable effecttsgo/global-console, effecttsgo/node-builtin-import, anti-slop-effect/no-service-constructor-imports -- JSON stdout is the subprocess test protocol; makeEventLog is the adapter constructor under process-boundary test. */
import { writeFileSync } from "node:fs";
import { AgentId, EventId, SessionId, type LogEntryInput } from "@humanlayer/fold-core";
import { StreamsWriter, type AppendOutcome } from "@streamsy/core";
import { Context, Effect } from "effect";
import { readHistory, sessionRefs, settle } from "../src/session-journal.ts";
import { openStore, type StreamsyStore } from "../src/storage.ts";
import { makeEventLog } from "../src/streamsy-event-log.ts";

const title = (value: string): LogEntryInput => ({
  _tag: "session_title",
  agentId: null,
  parentAgentId: null,
  toolCallId: null,
  title: value,
});

const started = (): LogEntryInput => ({
  _tag: "session_started",
  agentId: null,
  parentAgentId: null,
  toolCallId: null,
  cwd: null,
  sessionId: SessionId.create(),
  rootAgentId: AgentId.create(),
  meta: {},
});

const [command, filename, streamId, value = "worker", evidencePath] = process.argv.slice(2);
if (command === undefined || filename === undefined || streamId === undefined)
  throw new Error("usage: sqlite-process-worker <command> <filename> <stream-id> [value]");

const program = Effect.acquireUseRelease(
  openStore({ filename }),
  (store) =>
    Effect.gen(function* () {
      if (command === "create") {
        const log = yield* makeEventLog({ store, streamId, mode: "create" });
        const entry = yield* log.append(started());
        return { status: "created", eventId: entry.eventId };
      }
      if (command === "append") {
        const log = yield* makeEventLog({ store, streamId, mode: "resume" });
        const entry = yield* log.append(title(value));
        return { status: "appended", eventId: entry.eventId };
      }
      if (command === "takeover") {
        const log = yield* makeEventLog({ store, streamId, mode: "takeover" });
        const entry = yield* log.append(title(value));
        return { status: "taken-over", eventId: entry.eventId };
      }
      if (command === "crash-after-log-append") {
        const refs = sessionRefs(streamId);
        const writer = Context.get(store.context, StreamsWriter);
        const crashing: StreamsyStore = {
          ...store,
          context: Context.add(
            store.context,
            StreamsWriter,
            StreamsWriter.of({
              ...writer,
              append: (id, options) =>
                Effect.gen(function* () {
                  if (id === refs.log.id && evidencePath !== undefined)
                    writeFileSync(evidencePath, options.data);
                  const result = yield* writer.append(id, options);
                  if (id === refs.log.id && result.status === "appended") {
                    process.exit(86);
                  }
                  return result;
                }),
            }),
          ),
        };
        const log = yield* makeEventLog({ store: crashing, streamId, mode: "resume" });
        yield* log.append(title(value));
        return { status: "unexpected-survival", eventId: EventId.create() };
      }
      if (command === "settle-pending") {
        const refs = sessionRefs(streamId);
        const writer = Context.get(store.context, StreamsWriter);
        const observing: StreamsyStore = {
          ...store,
          context: Context.add(
            store.context,
            StreamsWriter,
            StreamsWriter.of({
              ...writer,
              append: (id, options) =>
                Effect.gen(function* () {
                  if (id === refs.log.id && evidencePath !== undefined)
                    writeFileSync(evidencePath, options.data);
                  const result = yield* writer.append(id, options);
                  return result;
                }),
            }),
          ),
        };
        const journal = yield* readHistory(refs.journal).pipe(Effect.provide(observing.context));
        const pending = journal.items.at(-1);
        if (pending?._tag !== "Pending")
          return yield* Effect.die(new Error("settle-pending requires a final Pending row"));
        const settlement: AppendOutcome = yield* settle(refs, pending).pipe(
          Effect.provide(observing.context),
        );
        return { status: "settled", settlement };
      }
      return yield* Effect.die(new Error(`unknown command: ${command}`));
    }),
  (store) => Effect.promise(() => store.close()),
);

console.log(JSON.stringify(await Effect.runPromise(Effect.scoped(program))));
