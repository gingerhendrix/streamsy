/* oxlint-disable effecttsgo/async-function -- bun:test owns these HTTP fixtures. */
import { afterEach, describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { streamNames } from "../domain/declaration.ts";
import { CatalogRowsResponse } from "../shared/api.ts";
import { IssueStore } from "../server/persistence/store.ts";
import { stateSourceId } from "../server/application/state-ingestion.ts";
import { call, host, json, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function fresh(): Host {
  const instance = host();
  open.push(instance);
  return instance;
}

const at = "2026-08-24T10:00:00.000Z";
const rows = {
  projects: {
    projectId: "p1",
    workspaceId: "main",
    key: "ENG",
    name: "Engineering",
    updatedAt: at,
  },
  users: { userId: "u1", workspaceId: "main", name: "Ada", updatedAt: at },
  labels: { labelId: "l1", workspaceId: "main", name: "Bug", color: "#ff0000", updatedAt: at },
  metadata: { workspaceId: "main", name: "Main workspace", updatedAt: at },
} as const;

describe("State source ingestion", () => {
  test("maintains four independently checkpointed current-row sources", async () => {
    const instance = fresh();
    const checkpoints: string[] = [];
    for (const collection of ["projects", "users", "labels", "metadata"] as const) {
      const key = collection === "metadata" ? "main" : Object.values(rows[collection])[0];
      const response = await json(
        await call(instance, "POST", `/api/workspaces/main/catalog/${collection}`, {
          key,
          value: rows[collection],
        }),
        CatalogRowsResponse,
      );
      expect(response.rows).toHaveLength(1);
      expect(response.folded).toBe(1);
      expect(response.changed).toBe(1);
      expect(response.checkpoint).toBeString();
      checkpoints.push(response.checkpoint ?? "");
    }
    expect(checkpoints).toHaveLength(4);
    expect(checkpoints.every((checkpoint) => checkpoint.length > 0)).toBe(true);
  });

  test("updates one key in place and repeated value delivery is logically idempotent", async () => {
    const instance = fresh();
    const path = "/api/workspaces/main/catalog/projects";
    await call(instance, "POST", path, { key: "p1", value: rows.projects });
    const updated = await json(
      await call(instance, "POST", path, {
        key: "p1",
        value: { ...rows.projects, name: "Platform", updatedAt: "2026-08-24T11:00:00.000Z" },
      }),
      CatalogRowsResponse,
    );
    expect(updated.rows).toHaveLength(1);
    expect(updated.changed).toBe(1);

    const repeated = await json(
      await call(instance, "POST", path, {
        key: "p1",
        value: { ...rows.projects, name: "Platform", updatedAt: "2026-08-24T11:00:00.000Z" },
      }),
      CatalogRowsResponse,
    );
    expect(repeated.rows).toHaveLength(1);
    expect(repeated.changed).toBe(0);
  });

  test("a poison boundary leaves rows and checkpoint unchanged", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/catalog/projects", {
      key: "p1",
      value: rows.projects,
    });
    const sourceId = stateSourceId("projects");
    const before = await instance.runtime.runPromise(
      Effect.gen(function* () {
        return yield* (yield* IssueStore).stateCheckpoint(sourceId, "main");
      }),
    );

    await instance.client
      .stream(streamNames.projects("main"))
      .appendJsonBatch([
        { type: "project", key: "wrong", value: rows.projects, headers: { operation: "upsert" } },
      ]);
    const poisoned = await call(instance, "GET", "/api/workspaces/main/catalog/projects");
    expect(poisoned.status).toBe(500);
    expect(await poisoned.json()).toMatchObject({ error: "source-poison" });

    const after = await instance.runtime.runPromise(
      Effect.gen(function* () {
        const store = yield* IssueStore;
        return {
          checkpoint: yield* store.stateCheckpoint(sourceId, "main"),
          rows: yield* store.stateRows(sourceId, "projects", "main"),
        };
      }),
    );
    expect(after.checkpoint).toBe(before);
    expect(after.rows).toEqual([rows.projects]);
  });

  test("validates a whole protocol batch before committing rows or checkpoint", async () => {
    const instance = fresh();
    const stream = instance.client.stream(streamNames.projects("main"));
    await stream.create({ contentType: "application/json" });
    await stream.appendJsonBatch([
      {
        type: "project",
        key: "p1",
        value: rows.projects,
        headers: { operation: "upsert" },
      },
      {
        type: "project",
        key: "wrong",
        value: { ...rows.projects, projectId: "p2" },
        headers: { operation: "upsert" },
      },
    ]);

    const poisoned = await call(instance, "GET", "/api/workspaces/main/catalog/projects");
    expect(poisoned.status).toBe(500);
    expect(await poisoned.json()).toMatchObject({ error: "source-poison" });

    const state = await instance.runtime.runPromise(
      Effect.gen(function* () {
        const store = yield* IssueStore;
        return {
          checkpoint: yield* store.stateCheckpoint(stateSourceId("projects"), "main"),
          rows: yield* store.stateRows(stateSourceId("projects"), "projects", "main"),
        };
      }),
    );
    expect(state).toEqual({ checkpoint: undefined, rows: [] });
  });

  test("State delete is a typed unsupported operation and does not advance", async () => {
    const instance = fresh();
    await instance.client
      .stream(streamNames.labels("main"))
      .create({ contentType: "application/json" });
    await instance.client
      .stream(streamNames.labels("main"))
      .appendJsonBatch([{ type: "label", key: "l1", headers: { operation: "delete" } }]);
    const response = await call(instance, "GET", "/api/workspaces/main/catalog/labels");
    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({ error: "unsupported-state-operation" });
    const checkpoint = await instance.runtime.runPromise(
      Effect.gen(function* () {
        return yield* (yield* IssueStore).stateCheckpoint(stateSourceId("labels"), "main");
      }),
    );
    expect(checkpoint).toBeUndefined();
  });

  test("malformed envelopes, collection mismatches, and invalid values fail-stop", async () => {
    const cases = [
      { type: "project", key: "p1", value: rows.projects, headers: {} },
      { type: "user", key: "p1", value: rows.projects, headers: { operation: "upsert" } },
      {
        type: "project",
        key: "p1",
        value: { ...rows.projects, name: "" },
        headers: { operation: "upsert" },
      },
    ] as const;
    for (const value of cases) {
      const instance = fresh();
      const stream = instance.client.stream(streamNames.projects("main"));
      await stream.create({ contentType: "application/json" });
      await stream.appendJsonBatch([value]);
      const response = await call(instance, "GET", "/api/workspaces/main/catalog/projects");
      expect(response.status).toBe(500);
      expect(await response.json()).toMatchObject({ error: "source-poison" });
      const checkpoint = await instance.runtime.runPromise(
        Effect.gen(function* () {
          return yield* (yield* IssueStore).stateCheckpoint(stateSourceId("projects"), "main");
        }),
      );
      expect(checkpoint).toBeUndefined();
    }
  });
});
