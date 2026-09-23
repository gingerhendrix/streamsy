import {
  post,
  readBoard,
  requestJson,
  scratchDirectory,
  startServer,
  stopServer,
  waitForServer,
} from "./support.ts";

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
const scratch = await scratchDirectory("streamsy-issue-smoke");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;
try {
  let server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  await post(baseUrl, "/api/workspaces/live/commands", {
    type: "create",
    commandId: "smoke-1",
    issueId: "smoke",
    projectId: "streamsy",
    title: "Smoke",
    status: "todo",
  });
  const before = await requestJson<{ offsets: Record<string, string> }>(
    baseUrl,
    "/api/workspaces/live/status",
  );
  assert(Object.keys(before.offsets).length === 5, "checkpoint does not have five inputs");
  assert((await readBoard(baseUrl, "live")).rows.length === 1, "first row missing");
  await stopServer(server);
  server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  const resumed = await requestJson<{ offsets: Record<string, string> }>(
    baseUrl,
    "/api/workspaces/live/status",
  );
  assert(
    JSON.stringify(before.offsets) === JSON.stringify(resumed.offsets),
    "checkpoint changed on restart",
  );
  await post(baseUrl, "/api/workspaces/live/commands", {
    type: "status",
    commandId: "smoke-2",
    issueId: "smoke",
    status: "done",
  });
  assert((await readBoard(baseUrl, "live")).rows[0]?.status === "done", "second command missing");
  await stopServer(server);
  console.log("smoke:http ok: checkpoint and row resumed, then advanced once");
} finally {
  await scratch.remove();
}
