/* oxlint-disable anti-slop/no-unknown-parameters -- The test codec exercises the server's external row boundary. */
import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { defineStateSink } from "./contract.ts";
import { handleStateSink } from "./server.ts";

interface Row {
  readonly id: string;
}

const sink = defineStateSink({
  name: "test.rows",
  from: { schema: "test" },
  row: {
    decode: (value: unknown): Row => {
      if (!(value instanceof Object) || !("id" in value)) throw new Error("row has no id");
      return { id: String(value.id) };
    },
  },
  key: "id",
  route: "/state/:workspaceId/rows",
  params: { workspaceId: { decode: (value: string) => value } },
  collection: { name: "rows", type: "row", primaryKey: "id" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
});

describe("state-sink server handling", () => {
  test("passes decoded route params to the selected capability", async () => {
    let workspaceId: string | undefined;
    const response = await Effect.runPromise(
      handleStateSink(
        sink,
        new Request("http://localhost/state/main/rows", {
          headers: { "x-streamsy-state-sink-reset": "snapshot" },
        }),
        {
          snapshot: (params) => {
            workspaceId = params.workspaceId;
            return Effect.succeed({ rows: [], offset: "-1" });
          },
          suffix: () => Effect.succeed(new Response("[]")),
        },
      ),
    );
    expect(response.status).toBe(200);
    expect(workspaceId).toBe("main");
  });

  test("an unsupported version is typed before transport access", async () => {
    let reads = 0;
    const response = await Effect.runPromise(
      handleStateSink(
        sink,
        new Request("http://localhost/state/main/rows", {
          headers: { "x-streamsy-state-sink-version": "7" },
        }),
        {
          snapshot: () => Effect.succeed({ rows: [], offset: "-1" }),
          suffix: () => {
            reads += 1;
            return Effect.succeed(new Response("[]"));
          },
        },
      ),
    );
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({
      _tag: "ProtocolVersionUnsupported",
      supported: 1,
    });
    expect(reads).toBe(0);
  });

  test("a supplied contract mismatch is typed before data access", async () => {
    let reads = 0;
    const response = await Effect.runPromise(
      handleStateSink(
        sink,
        new Request("http://localhost/state/main/rows", {
          headers: { "x-streamsy-state-sink-contract": "retired-contract" },
        }),
        {
          snapshot: () => {
            reads += 1;
            return Effect.succeed({ rows: [], offset: "-1" });
          },
          suffix: () => {
            reads += 1;
            return Effect.succeed(new Response("[]"));
          },
        },
      ),
    );
    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({
      _tag: "ResumeRejected",
      sink: "test.rows",
      reason: "contract-changed",
      recovery: "snapshot-then-live",
    });
    expect(reads).toBe(0);
  });
});
