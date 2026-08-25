/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow, and the consumer under test is the browser's Promise-native Durable Streams client. */
/**
 * The consumer binding, over real HTTP.
 *
 * This is the browser's own path: a caller-owned `DurableStream` pointed at the
 * declared sink route, wrapped by `createStreamDB`, feeding TanStack DB
 * collections. It runs against a real Bun server so the transport, the scope
 * param, native offsets and Durable State messages are all the ones a page
 * would see.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { memoryResumeStore } from "@streamsy/tanstack-db";
import { Effect } from "effect";
import { createBoardConnection, sortRows, type BoardConnection } from "../src/lib/board-db.ts";
import type { BoardIssuesRow } from "../src/generated/board-issues.ts";
import { IssueSink } from "../server/sink.ts";
import { call, createIssueBody, host, type Host } from "./support.ts";

interface Fixture {
  readonly instance: Host;
  readonly server: ReturnType<typeof Bun.serve>;
  readonly origin: string;
}

const fixtures: Fixture[] = [];
const connections: BoardConnection[] = [];

afterEach(async () => {
  for (const connection of connections.splice(0)) connection.close();
  for (const fixture of fixtures.splice(0)) {
    await fixture.server.stop(true);
    await fixture.instance.close();
  }
});

function serve(): Fixture {
  const instance = host();
  const server = Bun.serve({ port: 0, fetch: instance.fetch, idleTimeout: 30 });
  const fixture = { instance, server, origin: `http://localhost:${server.port}` };
  fixtures.push(fixture);
  return fixture;
}

function connect(fixture: Fixture): BoardConnection {
  const connection = createBoardConnection({
    workspaceId: "main",
    origin: fixture.origin,
    onStatus: () => undefined,
  });
  connections.push(connection);
  return connection;
}

function rowsOf(connection: BoardConnection): readonly BoardIssuesRow[] {
  // SAFETY: the collection's schema is the declared `IssueRow`, and StreamDB
  // decodes every row through it before writing, so `toArray` cannot contain a
  // value that schema rejected.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  return sortRows(connection.db.collections.issues.toArray);
}

/** Wait for the synchronized collection to satisfy a predicate, or fail loudly. */
async function until(
  connection: BoardConnection,
  predicate: (rows: readonly BoardIssuesRow[]) => boolean,
  what: string,
): Promise<readonly BoardIssuesRow[]> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const rows = rowsOf(connection);
    if (predicate(rows)) return rows;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what}; saw ${JSON.stringify(rows)}`);
    }
    await Bun.sleep(25);
  }
}

describe("the TanStack DB board binding", () => {
  test("preload synchronizes the maintained rows into the local collection", async () => {
    const fixture = serve();
    await call(fixture.instance, "POST", "/api/workspaces/main/seed");

    const resumeStore = memoryResumeStore();
    const connection = createBoardConnection({
      workspaceId: "main",
      origin: fixture.origin,
      onStatus: () => undefined,
      resumeStore,
    });
    connections.push(connection);
    await connection.preload();

    const rows = rowsOf(connection);
    expect(rows.map((row) => row.issueId).sort()).toEqual([
      "seed-maintain",
      "seed-plan",
      "seed-publish",
      "seed-scale",
    ]);
    expect(rows.every((row) => row.title.length > 0)).toBe(true);
    await Bun.sleep(0);
    expect((await resumeStore.load())?.offset).toBeString();
  });

  test("two live sessions converge on a create and a move without a refresh", async () => {
    const fixture = serve();
    const left = connect(fixture);
    const right = connect(fixture);
    await Promise.all([left.preload(), right.preload()]);

    await call(
      fixture.instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Seen by both", "todo"),
    );

    for (const connection of [left, right]) {
      const rows = await until(
        connection,
        (current) => current.some((row) => row.issueId === "issue-1"),
        "the created issue",
      );
      expect(rows.find((row) => row.issueId === "issue-1")?.status).toBe("todo");
    }

    await call(fixture.instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
      commandId: "cmd-2",
      status: "done",
    });

    for (const connection of [left, right]) {
      const rows = await until(
        connection,
        (current) => current.find((row) => row.issueId === "issue-1")?.status === "done",
        "the moved issue",
      );
      // The move updates the row in place; it does not add a second card.
      expect(rows.filter((row) => row.issueId === "issue-1")).toHaveLength(1);
    }
  });

  test("a second session started later rebuilds the board from the sink alone", async () => {
    const fixture = serve();
    await call(
      fixture.instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Before the reader existed", "in_progress"),
    );

    const later = connect(fixture);
    await later.preload();
    const rows = rowsOf(later);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.status).toBe("in_progress");
  });

  test("an invalid live offset resets first and removes a stale local row", async () => {
    const fixture = serve();
    await call(
      fixture.instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-current", "issue-current", "Authoritative row", "todo"),
    );
    const stale: BoardIssuesRow = {
      issueId: "issue-stale",
      projectId: "streamsy",
      projectName: "Streamsy",
      title: "Must disappear on reset",
      status: "backlog",
      assignee: "unassigned",
      updatedAt: "2026-08-25T00:00:00.000Z",
    };
    await fixture.instance.runtime.runPromise(
      Effect.gen(function* () {
        const sink = yield* IssueSink;
        yield* sink.publish("main", [{ kind: "enter", key: stale.issueId, after: stale }]);
      }),
    );

    let sinkReads = 0;
    const statuses: string[] = [];
    const connection = createBoardConnection({
      workspaceId: "main",
      origin: fixture.origin,
      onStatus: (status) => statuses.push(status.kind),
      // SAFETY: the adapter calls only the standard fetch signature; Bun's
      // ambient type adds `preconnect`, which this deterministic test wrapper
      // does not need to implement.
      fetch: ((input, init) => {
        const request = new Request(input, init);
        const url = new URL(request.url);
        if (url.pathname.startsWith("/state/")) {
          sinkReads += 1;
          if (sinkReads === 2) url.searchParams.set("offset", "not-an-offset");
        }
        return globalThis.fetch(new Request(url, request));
      }) as typeof globalThis.fetch,
    });
    connections.push(connection);
    await connection.preload();
    await until(
      connection,
      (rows) =>
        statuses.includes("resetting") &&
        rows.some((row) => row.issueId === "issue-current") &&
        rows.every((row) => row.issueId !== "issue-stale"),
      "reset-first stale-row removal",
    );
    expect(statuses).toContain("resetting");
    expect(rowsOf(connection).map((row) => row.issueId)).toEqual(["issue-current"]);
  });
});
