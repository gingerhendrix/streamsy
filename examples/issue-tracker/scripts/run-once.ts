import { ManagedRuntime } from "effect";
import { Projection } from "@streamsy/projection";
import { applicationLayer } from "../server/host.ts";
import { tracker } from "../server/outputs.ts";
import { issueRows } from "../server/projection.ts";
const workspaceId = Bun.argv[2];
const databasePath = process.env.ISSUE_TRACKER_DB;
if (workspaceId === undefined || databasePath === undefined)
  throw new Error("usage: ISSUE_TRACKER_DB=<path> run-once.ts <workspaceId>");
const runtime = ManagedRuntime.make(applicationLayer(databasePath));
try {
  console.log(await runtime.runPromise(Projection.run(issueRows.member({ workspaceId }))));
  console.log(await runtime.runPromise(Projection.run(tracker.member({ workspaceId }))));
} finally {
  await runtime.dispose();
}
