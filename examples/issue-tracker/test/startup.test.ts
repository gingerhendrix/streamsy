import { expect, test } from "bun:test";
import { Streams } from "@streamsy/core";
import { Effect, ManagedRuntime } from "effect";
import { applicationLayer, createInputs } from "../server/host.ts";
import { refs, userStream } from "../server/streams.ts";
import {
  scratchDirectory,
  startServer,
  stopServer,
  waitForServer,
  post,
  readBoard,
} from "../scripts/support.ts";

test("a member fault at startup leaves HTTP and other workspaces available", async () => {
  const scratch = await scratchDirectory("tracker-startup-fault");
  let server: ReturnType<typeof startServer> | undefined;
  try {
    const producer = ManagedRuntime.make(applicationLayer(scratch.database));
    try {
      await producer.runPromise(
        Effect.gen(function* () {
          yield* createInputs(refs("acme"));
          yield* Streams.append(userStream("acme"), [
            {
              type: "user",
              key: "ada",
              value: {
                userId: "ada",
                workspaceId: "wrong",
                name: "Ada",
                updatedAt: "2026-09-23T10:00:00Z",
              },
              headers: { operation: "upsert" },
            },
          ]);
        }),
      );
    } finally {
      await producer.dispose();
    }
    const port = 20_000 + Math.floor(Math.random() * 20_000);
    const baseUrl = `http://127.0.0.1:${port}`;
    server = startServer(port, scratch.database);
    await waitForServer(baseUrl);
    expect((await fetch(`${baseUrl}/document/workspaces/acme/summary`)).status).toBe(503);
    expect((await fetch(`${baseUrl}/api/workspaces/acme/status`)).status).toBe(200);
    await post(baseUrl, "/api/workspaces/live/commands", {
      type: "create",
      commandId: "healthy-1",
      issueId: "healthy",
      projectId: "p1",
      title: "Healthy",
    });
    expect((await readBoard(baseUrl, "live")).rows[0]?.issueId).toBe("healthy");
    await stopServer(server);
    const logs = await new Response(server.stdout).text();
    expect(logs).toContain("projection startup failed");
    expect(logs).toContain("issue-rows");
    expect(logs).toContain("issue-tracker");
    server = undefined;
  } finally {
    if (server !== undefined) await stopServer(server);
    await scratch.remove();
  }
});
