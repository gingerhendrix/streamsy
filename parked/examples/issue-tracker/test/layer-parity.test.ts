/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is one declaration executed on two sets of layers. */
/**
 * One declaration, two sets of layers.
 *
 * The memory host and the SQLite host share the declaration, the plan, the
 * engine, the sink and the router. The only difference is which layers a host
 * picked. This suite drives the same command script through both and requires
 * the maintained rows — and the published product — to be identical.
 */
import { describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { CatalogRowsResponse, IssuesResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, temporaryDirectory, type Host } from "./support.ts";

const Published = Schema.Struct({
  type: Schema.optionalKey(Schema.String),
  key: Schema.optionalKey(Schema.String),
  value: Schema.optionalKey(
    Schema.Struct({
      status: Schema.optionalKey(Schema.String),
      title: Schema.optionalKey(Schema.String),
    }),
  ),
});
interface Published extends Schema.Schema.Type<typeof Published> {}

/** The same script of commands, whatever the host is made of. */
async function drive(instance: Host): Promise<void> {
  const at = "2026-08-24T10:00:00.000Z";
  await call(instance, "POST", "/api/workspaces/main/catalog/projects", {
    key: "p1",
    value: {
      projectId: "p1",
      workspaceId: "main",
      key: "ENG",
      name: "Engineering",
      updatedAt: at,
    },
  });
  await call(instance, "POST", "/api/workspaces/main/catalog/users", {
    key: "u1",
    value: { userId: "u1", workspaceId: "main", name: "Ada", updatedAt: at },
  });
  await call(instance, "POST", "/api/workspaces/main/catalog/labels", {
    key: "l1",
    value: {
      labelId: "l1",
      workspaceId: "main",
      name: "Bug",
      color: "#ff0000",
      updatedAt: at,
    },
  });
  await call(instance, "POST", "/api/workspaces/main/catalog/metadata", {
    key: "main",
    value: { workspaceId: "main", name: "Main", updatedAt: at },
  });
  await call(
    instance,
    "POST",
    "/api/workspaces/main/issues",
    createIssueBody("cmd-1", "issue-1", "Declare", "backlog"),
  );
  await call(
    instance,
    "POST",
    "/api/workspaces/main/issues",
    createIssueBody("cmd-2", "issue-2", "Maintain", "todo"),
  );
  await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
    commandId: "cmd-3",
    status: "in_progress",
  });
  // A retry of a command already accepted, on both hosts.
  await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
    commandId: "cmd-3",
    status: "in_progress",
  });
}

/** The comparable shape of one host's product: no timestamps, no offsets. */
interface Product {
  readonly view: string;
  readonly planHash: string;
  readonly rows: readonly {
    readonly issueId: string;
    readonly projectId: string;
    readonly title: string;
    readonly status: string;
  }[];
  readonly published: readonly { readonly key?: string; readonly status?: string }[];
  readonly catalog: readonly { readonly collection: string; readonly rows: readonly unknown[] }[];
}

/** The current product, with the fields a durable clock would otherwise perturb removed. */
async function product(instance: Host): Promise<Product> {
  const listed = await json(
    await call(instance, "GET", "/api/workspaces/main/issues"),
    IssuesResponse,
  );
  const sink = await instance.fetch(new Request("http://localhost/state/workspaces/main/issues"));
  const published = (
    await Schema.decodePromise(Schema.fromJsonString(Schema.Array(Published)))(await sink.text())
  )
    .filter((message) => message.type === "issue")
    .map((message) => ({ key: message.key, status: message.value?.status }));
  const catalog = await Promise.all(
    ["projects", "users", "labels", "metadata"].map(async (collection) => {
      const response = await json(
        await call(instance, "GET", `/api/workspaces/main/catalog/${collection}`),
        CatalogRowsResponse,
      );
      return { collection, rows: response.rows };
    }),
  );

  return {
    view: listed.view,
    planHash: listed.planHash,
    rows: listed.rows.map((row) => ({
      issueId: row.issueId,
      projectId: row.projectId,
      title: row.title,
      status: row.status,
    })),
    published,
    catalog,
  };
}

describe("layer parity", () => {
  test("the memory host and the SQLite host maintain and publish the same product", async () => {
    const memory = host();
    const durable = host({ databaseDirectory: temporaryDirectory("issue-tracker-parity") });
    try {
      await drive(memory);
      await drive(durable);
      expect(await product(durable)).toEqual(await product(memory));
    } finally {
      await memory.close();
      await durable.close();
    }
  });
});
