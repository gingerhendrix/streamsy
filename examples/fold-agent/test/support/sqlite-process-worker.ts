/* oxlint-disable effecttsgo/global-console, anti-slop-effect/no-service-constructor-imports -- JSON stdout is the subprocess test protocol; makeEventLog is the adapter under process-boundary test. */
import { AgentId, SessionId, type LogEntryInput } from "@humanlayer/fold-core";
import { Effect } from "effect";
import { openStore } from "../../src/storage.ts";
import { makeEventLog } from "../../src/streamsy-event-log.ts";

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

const [command, filename, streamId, value = "worker"] = process.argv.slice(2);
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
      return yield* Effect.die(new Error(`unknown command: ${command}`));
    }),
  (store) => Effect.promise(() => store.close()),
);

console.log(JSON.stringify(await Effect.runPromise(Effect.scoped(program))));
