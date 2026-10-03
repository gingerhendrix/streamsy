/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/**
 * The issue-tracker SQL on every dialect: the schema, the issue upsert, the
 * ignored duplicate inserts, the label sequence rule, the catalog upsert and
 * delete, and the next command sequence. SQLite always runs. Postgres and
 * MySQL run in a fresh database when `STREAMSY_PG_URL` or `STREAMSY_MYSQL_URL`
 * is set, and skip otherwise.
 *
 * Run: `bun run --cwd examples/issue-tracker test:dialects`.
 */
import { describe, expect, test } from "bun:test";
import { MysqlClient } from "@effect/sql-mysql2";
import { PgClient } from "@effect/sql-pg";
import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { SqlStorage } from "@streamsy/storage";
import { Config, Context, Effect, Layer, Option, Random, Redacted, type Scope } from "effect";
import type { Reactivity } from "effect/unstable/reactivity";
import { SqlClient, type SqlError } from "effect/unstable/sql";
import type { IssueEvent, IssueLabelEvent, IssueStatus } from "../domain/issue.ts";
import {
  applicationLayer,
  createInputs,
  sqliteStorage,
  sqlStorage,
  type HostStorage,
} from "../server/host.ts";
import { issueRows, storedIssue } from "../server/projection.ts";
import { transact } from "../server/state.ts";
import { Events, LabelEvents, Users } from "../server/streams.ts";

const TIMEOUT_MS = 60_000;

const optionalUrl = (name: string) =>
  Effect.runSync(Config.option(Config.String(name))).pipe(Option.getOrUndefined);

interface ServerDialect {
  readonly name: "pg" | "mysql";
  readonly url: string | undefined;
  readonly client: (
    url: string,
  ) => Effect.Effect<SqlClient.SqlClient, SqlError.SqlError, Scope.Scope | Reactivity.Reactivity>;
  readonly drop: (name: string) => string;
}

const servers: ReadonlyArray<ServerDialect> = [
  {
    name: "pg",
    url: optionalUrl("STREAMSY_PG_URL"),
    client: (url) => PgClient.make({ url: Redacted.make(url), maxConnections: 1 }),
    drop: (name) => `DROP DATABASE IF EXISTS ${name} WITH (FORCE)`,
  },
  {
    name: "mysql",
    url: optionalUrl("STREAMSY_MYSQL_URL"),
    client: (url) => MysqlClient.make({ url: Redacted.make(url), maxConnections: 1 }),
    drop: (name) => `DROP DATABASE IF EXISTS ${name}`,
  },
];

/** A `streamsy_` database on the server of `baseUrl`, dropped when the scope closes. */
const freshDatabase = (dialect: ServerDialect, baseUrl: string) =>
  Effect.gen(function* () {
    const name = `streamsy_tracker_${yield* Random.nextIntBetween(0, 2 ** 31)}`;
    const admin = Context.get(
      yield* Layer.build(SqlStorage.clientLayer(dialect.client(baseUrl))),
      SqlClient.SqlClient,
    );
    yield* Effect.acquireRelease(admin.unsafe(`CREATE DATABASE ${name}`), () =>
      admin.unsafe(dialect.drop(name)).pipe(Effect.ignore),
    );
    const url = new URL(baseUrl);
    url.pathname = `/${name}`;
    return url.toString();
  });

const at = "2026-09-20T10:00:00.000Z";

const statusChanged = (eventId: string, sequence: number, status: IssueStatus): IssueEvent => ({
  type: "IssueStatusChanged",
  eventId,
  workspaceId: "live",
  issueId: "i1",
  status,
  sequence,
  occurredAt: at,
});

const assign = (eventId: string, sequence: number, assigneeId: string): IssueEvent => ({
  type: "IssueAssigned",
  eventId,
  workspaceId: "live",
  issueId: "i1",
  status: "done",
  assigneeId,
  sequence,
  occurredAt: at,
});

const label = (
  type: IssueLabelEvent["type"],
  membershipId: string,
  sequence: number,
): IssueLabelEvent => ({
  type,
  eventId: `${membershipId}-${sequence}`,
  workspaceId: "live",
  issueId: "i1",
  labelId: membershipId.split(".")[1] ?? "",
  membershipId,
  sequence,
  occurredAt: `2026-09-20T10:00:0${sequence}.000Z`,
});

const ada = {
  userId: "ada",
  workspaceId: "live",
  name: "Ada",
  updatedAt: "2026-09-20T10:00:00.000Z",
};

const scenario = Effect.gen(function* () {
  yield* createInputs("live");
  const sql = yield* SqlClient.SqlClient;
  const rows = issueRows.at({ workspaceId: "live" });

  yield* (yield* Events).append({ workspaceId: "live" }, [
    {
      type: "IssueCreated",
      eventId: "e1",
      workspaceId: "live",
      issueId: "i1",
      projectId: "p1",
      title: "Issue",
      status: "todo",
      sequence: 3,
      occurredAt: at,
    },
    statusChanged("e2", 4, "in_progress"),
    // The same event id again: the issue moves on, the change log keeps one row.
    statusChanged("e2", 5, "done"),
    assign("e3", 6, "ada"),
    assign("e3", 7, "bob"),
  ]);
  yield* (yield* LabelEvents).append({ workspaceId: "live" }, [
    label("LabelAttached", "i1.bug", 2),
    // Older than the attach: ignored.
    label("LabelDetached", "i1.bug", 1),
    label("LabelAttached", "i1.ui", 1),
    label("LabelDetached", "i1.ui", 3),
  ]);
  const users = Users.at({ workspaceId: "live" });
  yield* (yield* Users).append(
    { workspaceId: "live" },
    DurableState.changes(users, { offset: ZERO_OFFSET }, [
      DurableState.upsert("user", ada),
      DurableState.upsert("user", { ...ada, name: "Ada L." }),
    ]),
  );
  yield* Projection.run(rows);

  expect(yield* storedIssue(sql, "live", "i1")).toEqual({
    workspaceId: "live",
    issueId: "i1",
    projectId: "p1",
    title: "Issue",
    status: "done",
    sequence: 7,
    updatedAt: at,
    assigneeId: "bob",
  });
  const changes = yield* sql<{
    readonly event_id: string;
  }>`SELECT event_id FROM issue_changes ORDER BY id`;
  expect(changes.map((row) => row.event_id)).toEqual(["e1", "e2", "e3"]);
  const drafts = yield* sql<{
    readonly assignee_id: string;
  }>`SELECT assignee_id FROM notification_drafts`;
  expect(drafts.map((row) => row.assignee_id)).toEqual(["ada"]);
  const memberships = yield* sql<{
    readonly membership_id: string;
    readonly attached: number;
    readonly sequence: number;
  }>`SELECT membership_id, attached, sequence FROM issue_labels ORDER BY membership_id`;
  expect(memberships.map((row) => [row.membership_id, row.attached, row.sequence])).toEqual([
    ["i1.bug", 1, 2],
    ["i1.ui", 0, 3],
  ]);
  const stored = yield* sql<{ readonly value: string }>`SELECT value FROM users`;
  expect(stored.map((row) => JSON.parse(row.value).name)).toEqual(["Ada L."]);

  yield* (yield* Users).append(
    { workspaceId: "live" },
    DurableState.changes(
      users,
      { offset: (yield* (yield* Users).head({ workspaceId: "live" })).nextOffset },
      [DurableState.remove("user", ada.userId)],
    ),
  );
  yield* Projection.run(rows);
  expect(yield* sql`SELECT * FROM users`).toEqual([]);

  const next = yield* transact("live", {
    type: "status",
    commandId: "c1",
    issueId: "i1",
    status: "todo",
  });
  expect(next.event.sequence).toBe(8);
});

const run = <E>(storage: Layer.Layer<HostStorage, E>) =>
  Effect.runPromise(
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test owns its host.
    Effect.scoped(scenario.pipe(Effect.provide(applicationLayer(storage)))),
  );

describe("issue-tracker SQL", () => {
  test(
    "sqlite",
    async () => {
      await run(sqliteStorage(":memory:"));
    },
    TIMEOUT_MS,
  );

  for (const dialect of servers) {
    const baseUrl = dialect.url;
    test.skipIf(baseUrl === undefined)(
      dialect.name,
      async () => {
        if (baseUrl === undefined) return;
        await Effect.runPromise(
          Effect.scoped(
            Effect.flatMap(freshDatabase(dialect, baseUrl), (url) =>
              Effect.promise(() => run(sqlStorage(dialect.client(url)))),
            ),
          ),
        );
      },
      TIMEOUT_MS,
    );
  }
});
