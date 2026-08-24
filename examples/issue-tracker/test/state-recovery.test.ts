/* oxlint-disable effecttsgo/async-function -- bun:test owns the restart fixture. */
import { afterEach, describe, expect, test } from "bun:test";
import { CatalogRowsResponse } from "../shared/api.ts";
import { call, host, json, temporaryDirectory, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

describe("State source recovery", () => {
  test("SQLite resumes each source strictly after its committed checkpoint", async () => {
    const directory = temporaryDirectory("issue-tracker-state-recovery");
    const first = host({ databaseDirectory: directory });
    open.push(first);
    const at = "2026-08-24T10:00:00.000Z";
    const fixtures = [
      [
        "projects",
        "p1",
        { projectId: "p1", workspaceId: "main", key: "ENG", name: "Engineering", updatedAt: at },
      ],
      ["users", "u1", { userId: "u1", workspaceId: "main", name: "Ada", updatedAt: at }],
      [
        "labels",
        "l1",
        { labelId: "l1", workspaceId: "main", name: "Bug", color: "#ff0000", updatedAt: at },
      ],
      ["metadata", "main", { workspaceId: "main", name: "Main", updatedAt: at }],
    ] as const;
    for (const [collection, key, value] of fixtures) {
      await call(first, "POST", `/api/workspaces/main/catalog/${collection}`, { key, value });
    }
    open.splice(open.indexOf(first), 1);
    await first.close();

    const restarted = host({ databaseDirectory: directory });
    open.push(restarted);
    for (const [collection] of fixtures) {
      const response = await json(
        await call(restarted, "GET", `/api/workspaces/main/catalog/${collection}`),
        CatalogRowsResponse,
      );
      expect(response.rows).toHaveLength(1);
      expect(response.folded).toBe(0);
      expect(response.changed).toBe(0);
    }
  });
});
