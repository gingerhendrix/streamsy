import { ManagedRuntime } from "effect";
import { Projection } from "@streamsy/projection";
import { applicationLayer } from "../server/host.ts";
import { issueRows } from "../server/projection.ts";
const workspaceId = Bun.argv[2];
if (workspaceId === undefined) throw new Error("usage: run-once.ts <workspaceId>");
const runtime = ManagedRuntime.make(
  applicationLayer(process.env.ISSUE_TRACKER_DB ?? "issue-tracker.sqlite"),
);
try {
  console.log(await runtime.runPromise(Projection.run(issueRows.member({ workspaceId }))));
} finally {
  await runtime.dispose();
}
