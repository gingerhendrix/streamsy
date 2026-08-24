/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the Effect application across a process restart against on-disk SQLite. */
/**
 * Restart recovery.
 *
 * The same declaration, the same plan, the same router — with the durable log
 * and the maintained state both on disk. A restarted host must serve the rows
 * it already committed, resume strictly after its checkpoint, and not fold a
 * single event twice.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import { CommandResponse, IssuesResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, temporaryDirectory, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function durable(directory: string): Host {
  const created = host({ databaseDirectory: directory });
  open.push(created);
  return created;
}

async function close(instance: Host): Promise<void> {
  open.splice(open.indexOf(instance), 1);
  await instance.close();
}

describe("durable recovery", () => {
  test("rows survive a restart and ingestion resumes without folding twice", async () => {
    const directory = temporaryDirectory("issue-tracker-recovery");

    const first = durable(directory);
    const created = await json(
      await call(
        first,
        "POST",
        "/api/workspaces/main/issues",
        createIssueBody("cmd-1", "issue-1", "Survive a restart", "todo"),
      ),
      CommandResponse,
    );
    expect(created.maintenance.folded).toBe(1);
    await close(first);

    const restarted = durable(directory);
    const listed = await json(
      await call(restarted, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows.map((row) => row.issueId)).toEqual(["issue-1"]);
    expect(listed.rows[0]?.status).toBe("todo");

    // The restarted host reads its committed checkpoint, so the event it
    // already folded is not folded again.
    const moved = await json(
      await call(restarted, "POST", "/api/workspaces/main/issues/issue-1/status", {
        commandId: "cmd-2",
        status: "done",
      }),
      CommandResponse,
    );
    expect(moved.maintenance.folded).toBe(1);
    expect(moved.row?.status).toBe("done");

    const after = await json(
      await call(restarted, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(after.rows).toHaveLength(1);
    expect(after.rows[0]?.status).toBe("done");
  });

  test("a command receipt survives the restart, so a retry still reconciles", async () => {
    const directory = temporaryDirectory("issue-tracker-receipts");
    const body = createIssueBody("cmd-1", "issue-1", "Retried across a restart");

    const first = durable(directory);
    const created = await json(
      await call(first, "POST", "/api/workspaces/main/issues", body),
      CommandResponse,
    );
    await close(first);

    const restarted = durable(directory);
    const retried = await json(
      await call(restarted, "POST", "/api/workspaces/main/issues", body),
      CommandResponse,
    );
    expect(retried.reconciled).toBe(true);
    expect(retried.ack.offset).toBe(created.ack.offset);
    expect(retried.maintenance.folded).toBe(0);

    const listed = await json(
      await call(restarted, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toHaveLength(1);
  });

  test("source numbering continues from durable facts rather than a process counter", async () => {
    const directory = temporaryDirectory("issue-tracker-sequence");

    const first = durable(directory);
    await call(
      first,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "First"),
    );
    const second = await json(
      await call(
        first,
        "POST",
        "/api/workspaces/main/issues",
        createIssueBody("cmd-2", "issue-2", "Second"),
      ),
      CommandResponse,
    );
    expect(second.sequence).toBe(1);
    await close(first);

    const restarted = durable(directory);
    const third = await json(
      await call(
        restarted,
        "POST",
        "/api/workspaces/main/issues",
        createIssueBody("cmd-3", "issue-3", "Third"),
      ),
      CommandResponse,
    );
    expect(third.sequence).toBe(2);
  });

  test("upgrades Slice 1 receipts to the workspace-scoped application schema", async () => {
    const directory = temporaryDirectory("issue-tracker-receipt-migration");
    const filename = join(directory, "view.sqlite");
    const legacy = new Database(filename, { create: true });
    legacy.exec(`CREATE TABLE command_receipts (
      command_id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL,
      issue_id TEXT NOT NULL,
      offset_token TEXT NOT NULL,
      event_id TEXT NOT NULL,
      sequence INTEGER NOT NULL
    )`);
    legacy.close(false);

    const migrated = durable(directory);
    const created = await call(
      migrated,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-new", "issue-new", "After migration"),
    );
    expect(created.status).toBe(201);
    await close(migrated);

    const checked = new Database(filename);
    const columns = checked
      .query<{ name: string }, []>("PRAGMA table_info(command_receipts)")
      .all()
      .map((column) => column.name);
    checked.close(false);
    expect(columns).toContain("request_hash");
    expect(columns).toContain("event_offset");
  });
});
