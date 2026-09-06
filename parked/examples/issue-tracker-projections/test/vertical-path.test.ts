/* oxlint-disable effecttsgo/async-function -- Vitest owns this file's control flow: every `test` and `afterEach` callback is a Promise the runner awaits, and the shared helpers are Promise-native drivers over the host's Web `fetch` handler. The behaviour under test is the Effect application behind that HTTP surface, which the host's own ManagedRuntime runs. */
import { afterEach, describe, expect, test } from "vitest";
import type { JsonValue } from "@streamsy/core";
import { Schema } from "effect";
import {
  BoardResponse,
  CoverageResponse,
  MutationResponse,
  ProjectsResponse,
  RepairResponse,
  SeedResponse,
} from "../shared/api.ts";
import { createLocalHost } from "../server/local.ts";

type Host = ReturnType<typeof createLocalHost>;

const hosts: Host[] = [];

afterEach(async () => {
  await Promise.all(hosts.splice(0).map((host) => host.close()));
});

function newHost(): Host {
  const host = createLocalHost();
  hosts.push(host);
  return host;
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

async function workspace(host: Host, workspaceId: string, projectId = "launch"): Promise<void> {
  await call(host, "POST", `/api/workspaces/${workspaceId}/projects`, {
    projectId,
    projectKey: "SHIP",
    name: "Launch",
  });
}

describe("IssueEvents → IssueDetail → ProjectBoard", () => {
  test("a created issue returns an exact ack and proves both hops", async () => {
    const host = newHost();
    await workspace(host, "w1");
    const created = await json(
      await call(host, "POST", "/api/workspaces/w1/issues", {
        commandId: "cmd-1",
        issueId: "issue-1",
        projectId: "launch",
        title: "Prove the path",
      }),
      MutationResponse,
    );

    expect(created.ack.stream).toBe("workspaces/w1/issues/issue-1/events");
    expect(created.ack.position.length).toBeGreaterThan(0);
    expect(created.coverage.status).toBe("proven");
    expect(created.coverage.hops.map((hop) => hop.label)).toEqual([
      "issue-detail",
      "project-board",
    ]);
    expect(created.detail).toMatchObject({ issueKey: "SHIP-100", status: "backlog" });
  });

  test("edits reach the board and the board is rebuilt from durable State", async () => {
    const host = newHost();
    await workspace(host, "w2");
    await call(host, "POST", "/api/workspaces/w2/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "launch",
      title: "Prove the path",
    });
    const moved = await json(
      await call(host, "POST", "/api/workspaces/w2/issues/issue-1/commands", {
        commandId: "cmd-2",
        type: "status",
        status: "in-progress",
      }),
      MutationResponse,
    );
    expect(moved.coverage.status).toBe("proven");

    const board = await json(
      await call(host, "GET", "/api/workspaces/w2/projects/launch/board"),
      BoardResponse,
    );
    expect(board.rows).toHaveLength(1);
    expect(board.rows[0]).toMatchObject({ issueId: "issue-1", status: "in-progress" });
    // The board response is folded from the durable board State stream, never
    // from a command-response cache.
    expect(board.boardStream).toBe("workspaces/w2/projects/launch/board");
  });

  test("a repeated command reconciles to the original offset", async () => {
    const host = newHost();
    await workspace(host, "w3");
    const first = await json(
      await call(host, "POST", "/api/workspaces/w3/issues", {
        commandId: "cmd-1",
        issueId: "issue-1",
        projectId: "launch",
        title: "Prove the path",
      }),
      MutationResponse,
    );
    const again = await json(
      await call(host, "POST", "/api/workspaces/w3/issues", {
        commandId: "cmd-1",
        issueId: "issue-1",
        projectId: "launch",
        title: "Prove the path",
      }),
      MutationResponse,
    );
    expect(again.reconciled).toBe(true);
    expect(again.ack.position).toBe(first.ack.position);

    const board = await json(
      await call(host, "GET", "/api/workspaces/w3/projects/launch/board"),
      BoardResponse,
    );
    expect(board.rows).toHaveLength(1);
  });

  test("two projects keep independent boards", async () => {
    const host = newHost();
    await workspace(host, "w4", "launch");
    await call(host, "POST", "/api/workspaces/w4/projects", {
      projectId: "platform",
      projectKey: "PLAT",
      name: "Platform",
    });
    await call(host, "POST", "/api/workspaces/w4/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "launch",
      title: "Launch work",
    });
    await call(host, "POST", "/api/workspaces/w4/issues", {
      commandId: "cmd-2",
      issueId: "issue-2",
      projectId: "platform",
      title: "Platform work",
    });

    const launch = await json(
      await call(host, "GET", "/api/workspaces/w4/projects/launch/board"),
      BoardResponse,
    );
    const platform = await json(
      await call(host, "GET", "/api/workspaces/w4/projects/platform/board"),
      BoardResponse,
    );
    expect(launch.rows.map((row) => row.issueId)).toEqual(["issue-1"]);
    expect(platform.rows.map((row) => row.issueId)).toEqual(["issue-2"]);
    expect(platform.rows[0]!.issueKey).toBe("PLAT-100");
  });

  test("validation faults are 400 and unknown resources are 404", async () => {
    const host = newHost();
    await workspace(host, "w5");
    expect(
      (
        await call(host, "POST", "/api/workspaces/w5/issues", {
          commandId: "cmd-1",
          issueId: "bad/id",
          projectId: "launch",
          title: "Nope",
        })
      ).status,
    ).toBe(400);
    expect((await call(host, "GET", "/api/workspaces/w5/issues/missing")).status).toBe(404);
    expect(
      (
        await call(host, "POST", "/api/workspaces/w5/issues", {
          commandId: "cmd-2",
          issueId: "issue-9",
          projectId: "absent",
          title: "Nope",
        })
      ).status,
    ).toBe(404);
  });

  test("a repeated project create reconciles to the durable row, not the new payload", async () => {
    const host = newHost();
    const created = await call(host, "POST", "/api/workspaces/w6/projects", {
      projectId: "launch",
      projectKey: "SHIP",
      name: "Launch",
    });
    expect(created.status).toBe(201);

    // Same project id, changed payload. The producer sequence was already
    // accepted, so the durable row answers and payload equality is not claimed.
    const again = await call(host, "POST", "/api/workspaces/w6/projects", {
      projectId: "launch",
      projectKey: "MOVE",
      name: "Renamed after the fact",
    });
    expect(again.status).toBe(200);
    expect(await again.json()).toEqual({
      projectId: "launch",
      projectKey: "SHIP",
      name: "Launch",
    });

    const listed = await json(
      await call(host, "GET", "/api/workspaces/w6/projects"),
      ProjectsResponse,
    );
    expect(listed.projects).toEqual([{ projectId: "launch", projectKey: "SHIP", name: "Launch" }]);
  });

  test("an invalid project request is a 400, not a silent 201", async () => {
    const host = newHost();
    expect(
      (
        await call(host, "POST", "/api/workspaces/w7/projects", {
          projectId: "bad/id",
          projectKey: "SHIP",
          name: "Launch",
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await call(host, "POST", "/api/workspaces/w7/projects", {
          projectId: "launch",
          projectKey: "SHIP",
          name: "   ",
        })
      ).status,
    ).toBe(400);
  });

  test("a deferred command is accepted unproven and converges through repair", async () => {
    const host = newHost();
    await workspace(host, "w8");
    await call(host, "POST", "/api/workspaces/w8/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "launch",
      title: "Prove the path",
    });

    // No immediate pass runs, so the response must not claim proven coverage.
    const deferred = await json(
      await call(host, "POST", "/api/workspaces/w8/issues/issue-1/commands?projections=deferred", {
        commandId: "cmd-2",
        type: "status",
        status: "done",
      }),
      MutationResponse,
    );
    expect(deferred.coverage.status).not.toBe("proven");
    expect(deferred.projections.map((pass) => pass.outcome)).toEqual(["deferred", "deferred"]);
    expect(deferred.ack.position.length).toBeGreaterThan(0);

    // The read-only probe agrees before any catch-up work has been done.
    const before = await json(
      await call(
        host,
        "GET",
        `/api/workspaces/w8/issues/issue-1/coverage?position=${encodeURIComponent(
          deferred.ack.position,
        )}`,
      ),
      CoverageResponse,
    );
    expect(before.coverage.status).not.toBe("proven");

    const repaired = await json(
      await call(host, "POST", "/api/workspaces/w8/projects/launch/repair"),
      RepairResponse,
    );
    expect(repaired.projections.every((pass) => pass.outcome === "caught-up")).toBe(true);

    const after = await json(
      await call(
        host,
        "GET",
        `/api/workspaces/w8/issues/issue-1/coverage?position=${encodeURIComponent(
          deferred.ack.position,
        )}`,
      ),
      CoverageResponse,
    );
    expect(after.coverage.status).toBe("proven");

    const board = await json(
      await call(host, "GET", "/api/workspaces/w8/projects/launch/board"),
      BoardResponse,
    );
    expect(board.rows[0]).toMatchObject({ issueId: "issue-1", status: "done" });
  });

  test("a coverage probe needs a position and a known issue", async () => {
    const host = newHost();
    await workspace(host, "w9");
    await call(host, "POST", "/api/workspaces/w9/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "launch",
      title: "Prove the path",
    });
    expect((await call(host, "GET", "/api/workspaces/w9/issues/issue-1/coverage")).status).toBe(
      400,
    );
    expect(
      (await call(host, "GET", "/api/workspaces/w9/issues/missing/coverage?position=0_0")).status,
    ).toBe(404);
  });

  test("every settled command reports a classified pass for both projections", async () => {
    const host = newHost();
    await workspace(host, "w10");
    const created = await json(
      await call(host, "POST", "/api/workspaces/w10/issues", {
        commandId: "cmd-1",
        issueId: "issue-1",
        projectId: "launch",
        title: "Prove the path",
      }),
      MutationResponse,
    );
    expect(created.projections).toEqual([
      { label: "issue-detail", status: "caught-up", outcome: "caught-up" },
      { label: "project-board", status: "caught-up", outcome: "caught-up" },
    ]);
  });

  test("the seeded workspace is complete and idempotent", async () => {
    const host = newHost();
    const first = await json(await call(host, "POST", "/api/workspaces/main/seed"), SeedResponse);
    const second = await json(await call(host, "POST", "/api/workspaces/main/seed"), SeedResponse);
    expect(second.issues).toEqual(first.issues);

    const board = await json(
      await call(host, "GET", "/api/workspaces/main/projects/launch/board"),
      BoardResponse,
    );
    expect(board.rows).toHaveLength(3);
    expect(new Set(board.rows.map((row) => row.status))).toEqual(
      new Set(["backlog", "in-progress", "done"]),
    );
  });
});
