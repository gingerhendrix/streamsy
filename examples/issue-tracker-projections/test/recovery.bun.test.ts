import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import type { BoardResponse, MutationResponse } from "../shared/api.ts";
import { createLocalHost } from "../server/local.ts";

type Host = ReturnType<typeof createLocalHost>;

const open: Host[] = [];

afterEach(async () => {
  await Promise.all(open.splice(0).map((host) => host.close()));
});

function hostFor(filename: string): Host {
  const host = createLocalHost({ adapter: createSqliteStorageAdapter({ filename }) });
  open.push(host);
  return host;
}

function tempDatabase(): string {
  return join(mkdtempSync(join(tmpdir(), "issue-tracker-recovery-")), "state.sqlite");
}

async function call(host: Host, method: string, path: string, body?: unknown): Promise<Response> {
  return host.fetch(
    new Request(`http://localhost${path}`, {
      method,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
    }),
  );
}

async function json<T>(response: Response): Promise<T> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 500)}`);
  return JSON.parse(text) as T;
}

async function setup(host: Host): Promise<MutationResponse> {
  await call(host, "POST", "/api/workspaces/main/projects", {
    projectId: "launch",
    projectKey: "SHIP",
    name: "Launch",
  });
  return json<MutationResponse>(
    await call(host, "POST", "/api/workspaces/main/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "launch",
      title: "Survive a restart",
    }),
  );
}

describe("durable recovery", () => {
  test("detail, membership, board, and coverage survive a host restart", async () => {
    const filename = tempDatabase();
    const first = hostFor(filename);
    const created = await setup(first);
    expect(created.coverage.status).toBe("proven");
    await first.close();
    open.splice(open.indexOf(first), 1);

    const restarted = hostFor(filename);
    const board = await json<BoardResponse>(
      await call(restarted, "GET", "/api/workspaces/main/projects/launch/board"),
    );
    expect(board.rows.map((row) => row.issueId)).toEqual(["issue-1"]);

    // A further command on the restarted host resumes strictly after durable
    // lineage and still proves the whole chain.
    const moved = await json<MutationResponse>(
      await call(restarted, "POST", "/api/workspaces/main/issues/issue-1/commands", {
        commandId: "cmd-2",
        type: "status",
        status: "done",
      }),
    );
    expect(moved.coverage.status).toBe("proven");
    expect(moved.detail?.status).toBe("done");

    const repaired = await json<{ repaired: string[] }>(
      await call(restarted, "POST", "/api/workspaces/main/projects/launch/repair"),
    );
    expect(repaired.repaired).toEqual(["issue-1"]);
  });

  test("repeated repair passes are no-ops once the board is caught up", async () => {
    const filename = tempDatabase();
    const host = hostFor(filename);
    await setup(host);
    const before = await json<BoardResponse>(
      await call(host, "GET", "/api/workspaces/main/projects/launch/board"),
    );
    await call(host, "POST", "/api/workspaces/main/projects/launch/repair");
    await call(host, "POST", "/api/workspaces/main/projects/launch/repair");
    const after = await json<BoardResponse>(
      await call(host, "GET", "/api/workspaces/main/projects/launch/board"),
    );
    expect(after).toEqual(before);
  });

  test("an unseeded workspace reports no projects rather than failing", async () => {
    const host = hostFor(tempDatabase());
    const projects = await json<{ projects: unknown[] }>(
      await call(host, "GET", "/api/workspaces/empty/projects"),
    );
    expect(projects.projects).toEqual([]);
    expect((await call(host, "GET", "/api/workspaces/empty/projects/none/board")).status).toBe(404);
  });
});
