import { State, Streams, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect, ManagedRuntime } from "effect";
import { applicationLayer, createInputs } from "../server/host.ts";
import { tracker } from "../server/outputs.ts";
import { labelEvents, labelStream, refs } from "../server/streams.ts";
import {
  post,
  readBoard,
  requestJson,
  scratchDirectory,
  startServer,
  stopServer,
  waitForServer,
} from "./support.ts";
import type { LabelCountRow, WorkspaceSummary } from "../domain/outputs.ts";
import type { IssueEvent } from "../domain/issue.ts";

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
const scratch = await scratchDirectory("streamsy-issue-serve");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;
let server: ReturnType<typeof startServer> | undefined;
try {
  const producer = ManagedRuntime.make(applicationLayer(scratch.database));
  try {
    await producer.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        const ref = labelStream("live");
        const label = {
          workspaceId: "live",
          labelId: "keep",
          name: "Keep",
          color: "#123456",
          updatedAt: "2026-09-23T10:00:00Z",
        };
        const removed = { ...label, labelId: "remove", name: "Remove" };
        yield* Streams.append(
          ref,
          State.changes(ref, { offset: ZERO_OFFSET }, [
            State.upsert("label", label),
            State.upsert("label", removed),
          ]),
        );
        yield* Projection.run(tracker.member({ workspaceId: "live" }));
        yield* Streams.append(
          ref,
          State.changes(ref, { offset: "1" }, [State.delete("label", removed)]),
        );
        yield* Streams.append(labelEvents.ref({ workspaceId: "live" }), [
          {
            type: "LabelAttached",
            workspaceId: "live",
            issueId: "smoke",
            labelId: "keep",
            membershipId: "smoke.keep",
            eventId: "label-1",
            sequence: 0,
            occurredAt: label.updatedAt,
          },
        ]);
        yield* Projection.run(tracker.member({ workspaceId: "live" }));
      }),
    );
  } finally {
    await producer.dispose();
  }
  server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  await post(baseUrl, "/api/workspaces/live/commands", {
    type: "create",
    commandId: "serve-1",
    issueId: "smoke",
    projectId: "p1",
    title: "Served",
    status: "todo",
  });
  await post(baseUrl, "/api/workspaces/live/commands", {
    type: "status",
    commandId: "serve-2",
    issueId: "smoke",
    status: "done",
  });
  const check = async () => {
    const board = await readBoard(baseUrl, "live");
    assert(board.rows.length === 1 && board.rows[0]?.status === "done", "board rows missing");
    assert(board.rows[0]?.labelIds.join() === "keep", "membership missing");
    const changes = await requestJson<
      Array<{
        key: string;
        value?: LabelCountRow;
        old_value?: LabelCountRow;
        headers: { operation: string };
      }>
    >(baseUrl, "/state/workspaces/live/label-counts?offset=-1");
    assert(
      changes.some(
        (change) =>
          change.key === "remove" &&
          change.headers.operation === "delete" &&
          change.old_value === undefined,
      ),
      "key-only output delete missing",
    );
    const counts = new Map<string, LabelCountRow>();
    for (const change of changes) {
      if (change.headers.operation === "delete") counts.delete(change.key);
      else if (change.value) counts.set(change.key, change.value);
    }
    assert(counts.size === 1 && counts.get("keep")?.count === 1, "label counts incorrect");
    const transitions = await requestJson<IssueEvent[]>(
      baseUrl,
      "/feed/workspaces/live/issue-transitions?offset=-1",
    );
    assert(
      transitions.map((event) => event.eventId).join() === "serve-1,serve-2",
      "transitions duplicated or missing",
    );
    const response = await fetch(`${baseUrl}/document/workspaces/live/summary`);
    assert(response.status === 200, "document unavailable");
    const summary: WorkspaceSummary = await response.json();
    assert(summary.issueCount === 1 && summary.doneCount === 1, "summary incorrect");
    const etag = response.headers.get("etag");
    assert(etag !== null, "document has no ETag");
    assert(
      (
        await fetch(`${baseUrl}/document/workspaces/live/summary`, {
          headers: { "if-none-match": etag },
        })
      ).status === 304,
      "document revalidation failed",
    );
    return etag;
  };
  const etag = await check();
  await stopServer(server);
  server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  assert((await check()) === etag, "document changed on restart");
  console.log("smoke:serve ok: rows, key-only delete, transitions, ETag/304 and SQLite restart");
} finally {
  if (server !== undefined) await stopServer(server);
  await scratch.remove();
}
