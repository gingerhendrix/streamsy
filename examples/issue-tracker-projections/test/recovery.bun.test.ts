/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow: every `test` and `afterEach` callback is a Promise the runner awaits, and the setup helpers are Promise-native drivers over the host's Web `fetch` handler. The behaviour under test is the Effect application behind that HTTP surface, exercised across a restart against one on-disk SQLite database. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The restart fixture needs a real on-disk SQLite database, so it creates its temporary directory with the Node-compatible filesystem API.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The same fixture resolves that database path with the Node-compatible path API.
import { join } from "node:path";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import type { JsonValue } from "@streamsy/core";
import { afterEach, describe, expect, test } from "bun:test";
import {
  BoardResponse,
  MutationResponse,
  ProjectsResponse,
  RepairResponse,
} from "../shared/api.ts";
import { Schema } from "effect";
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

async function call(host: Host, method: string, path: string, body?: JsonValue): Promise<Response> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "content-type": "application/json" };
  }
  return host.fetch(new Request(`http://localhost${path}`, init));
}

/**
 * Read a JSON body.
 *
 * The type parameter names the response contract the caller expects; the
 * server builds that contract from the shared Schemas, and the assertions in
 * this suite are what check it.
 */
async function json<S extends Schema.ConstraintDecoder<unknown>>(
  response: Response,
  schema: S,
): Promise<S["Type"]> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 500)}`);
  return Schema.decodeUnknownSync(schema)(JSON.parse(text));
}

async function setup(host: Host): Promise<MutationResponse> {
  await call(host, "POST", "/api/workspaces/main/projects", {
    projectId: "launch",
    projectKey: "SHIP",
    name: "Launch",
  });
  return json(
    await call(host, "POST", "/api/workspaces/main/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "launch",
      title: "Survive a restart",
    }),
    MutationResponse,
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
    const board = await json(
      await call(restarted, "GET", "/api/workspaces/main/projects/launch/board"),
      BoardResponse,
    );
    expect(board.rows.map((row) => row.issueId)).toEqual(["issue-1"]);

    // A further command on the restarted host resumes strictly after durable
    // lineage and still proves the whole chain.
    const moved = await json(
      await call(restarted, "POST", "/api/workspaces/main/issues/issue-1/commands", {
        commandId: "cmd-2",
        type: "status",
        status: "done",
      }),
      MutationResponse,
    );
    expect(moved.coverage.status).toBe("proven");
    expect(moved.detail?.status).toBe("done");

    const repaired = await json(
      await call(restarted, "POST", "/api/workspaces/main/projects/launch/repair"),
      RepairResponse,
    );
    expect(repaired.repaired).toEqual(["issue-1"]);
  });

  test("repeated repair passes are no-ops once the board is caught up", async () => {
    const filename = tempDatabase();
    const host = hostFor(filename);
    await setup(host);
    const before = await json(
      await call(host, "GET", "/api/workspaces/main/projects/launch/board"),
      BoardResponse,
    );
    await call(host, "POST", "/api/workspaces/main/projects/launch/repair");
    await call(host, "POST", "/api/workspaces/main/projects/launch/repair");
    const after = await json(
      await call(host, "GET", "/api/workspaces/main/projects/launch/board"),
      BoardResponse,
    );
    expect(after).toEqual(before);
  });

  test("an unseeded workspace reports no projects rather than failing", async () => {
    const host = hostFor(tempDatabase());
    const projects = await json(
      await call(host, "GET", "/api/workspaces/empty/projects"),
      ProjectsResponse,
    );
    expect(projects.projects).toEqual([]);
    expect((await call(host, "GET", "/api/workspaces/empty/projects/none/board")).status).toBe(404);
  });
});
