import { freePort } from "@streamsy/test-support/server";
import { BunServices } from "@effect/platform-bun";
import { layer as testLayer } from "@effect/vitest";
import { FetchHttpClient, HttpClient } from "effect/http";

import { expect } from "vitest";
import { Context, Effect, Layer, Option } from "effect";
import { applicationLayer, createInputs, hostLayer, sqliteStorage } from "../server/host.ts";
import { Users } from "../server/streams.ts";
import {
  scratchDirectory,
  startServer,
  stopServer,
  waitForServer,
  postCommand,
  readBoard,
} from "./support.ts";

testLayer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    it.effect("a workspace fault at startup leaves HTTP and other workspaces available", () =>
      Effect.gen(function* () {
        const scratch = yield* scratchDirectory("tracker-startup-fault");
        let server: ReturnType<typeof startServer> | undefined;

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (server !== undefined) yield* stopServer(server);
            yield* Effect.void;
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const producer = yield* Layer.build(applicationLayer(sqliteStorage(scratch.database)));

            yield* Effect.gen(function* () {
              yield* createInputs("acme");
              yield* (yield* Users).append({ workspaceId: "acme" }, [
                {
                  type: "user",
                  key: "ada",
                  value: {
                    userId: "ada",
                    workspaceId: "wrong",
                    name: "Ada",
                    updatedAt: "2026-09-23T10:00:00Z",
                  },
                  headers: { operation: "upsert" },
                },
              ]);
            }).pipe(Effect.provideContext(producer));
          }),
        );
        const port = yield* freePort;
        const baseUrl = `http://127.0.0.1:${port}`;
        server = startServer(port, scratch.database);
        yield* waitForServer(baseUrl);
        expect((yield* HttpClient.get(`${baseUrl}/workspaces/acme/summary`)).status).toBe(200);
        expect((yield* HttpClient.get(`${baseUrl}/api/workspaces/acme/status`)).status).toBe(200);
        yield* postCommand(baseUrl, "live", {
          type: "create",
          commandId: "healthy-1",
          issueId: "healthy",
          projectId: "p1",
          title: "Healthy",
        });
        expect((yield* readBoard(baseUrl, "live")).rows[0]?.issueId).toBe("healthy");
        yield* stopServer(server);
        const output = server.stdout;
        const logs = yield* Effect.promise(() => new Response(output).text());
        expect(logs).toContain("projection startup failed");
        expect(logs).toContain("issue-rows");
        expect(logs).toContain("bugsy");
        server = undefined;
      }),
    );

    it.effect("the host runtime provides application storage services and no Storage", () =>
      Effect.gen(function* () {
        const { Storage } = yield* Effect.promise(() => import("@streamsy/core"));

        const { CheckpointStore, Transaction } = yield* Effect.promise(
          () => import("@streamsy/projection"),
        );

        const { SqlClient } = yield* Effect.promise(() => import("effect/sql"));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(hostLayer(sqliteStorage(":memory:")));
            expect(Option.isNone(Context.getOption(context, Storage))).toBe(true);
            expect(Option.isSome(Context.getOption(context, CheckpointStore))).toBe(true);
            expect(Option.isSome(Context.getOption(context, Transaction))).toBe(true);
            expect(Option.isSome(Context.getOption(context, SqlClient.SqlClient))).toBe(true);
          }),
        );
      }),
    );
  },
);
