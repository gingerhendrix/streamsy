import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { authorizerLayer, SinkAuthorizationDenied } from "./authorization.ts";
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
  auth: { policy: "test", required: "rows:read" },
});

describe("state-sink server handling", () => {
  test("authorization runs before snapshot or suffix data access", async () => {
    let reads = 0;
    const response = await Effect.runPromise(
      handleStateSink(sink, new Request("http://localhost/state/main/rows"), {
        snapshot: () => {
          reads += 1;
          return Effect.succeed({ rows: [], offset: "-1" });
        },
        suffix: () => {
          reads += 1;
          return Effect.succeed(new Response("[]"));
        },
      }).pipe(
        Effect.provide(
          authorizerLayer(({ sink: target }) =>
            Effect.fail(new SinkAuthorizationDenied({ required: target.auth.required })),
          ),
        ),
      ),
    );
    expect(response.status).toBe(403);
    expect(reads).toBe(0);
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
      ).pipe(Effect.provide(authorizerLayer(() => Effect.succeed({ generation: "g1" })))),
    );
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({
      _tag: "ProtocolVersionUnsupported",
      supported: 1,
    });
    expect(reads).toBe(0);
  });
});
