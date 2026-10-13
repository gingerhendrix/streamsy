/* oxlint-disable effecttsgo/global-fetch, effecttsgo/global-fetch-in-effect -- Exercise the adapter standard Request transport against workerd. */
import { BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { Effect, Schema } from "effect";
import { createAgentFileSystem } from "../../agent/index.ts";
import { CreateFolder } from "../../server/domain.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "workerd requires the writer token and denied writes give EACCES without retry",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("worker-agent-");

        const host = yield* Effect.acquireRelease(
          Effect.promise(() => startWorker(directory, "agent-token")),
          (runtime) => Effect.promise(runtime.stop),
        );

        const body = yield* Schema.encodeEffect(Schema.fromJsonString(CreateFolder))({
          folderId: "docs",
          title: "Docs",
        });

        const created = yield* Effect.promise(() =>
          fetch(`${host.url}/api/folders`, {
            method: "POST",
            headers: { authorization: "Bearer agent-token", "content-type": "application/json" },
            body,
          }),
        );

        expect(created.status).toBe(200);
        yield* Effect.promise(() => created.text());

        for (const token of [undefined, "wrong", "agent-token"]) {
          let writes = 0;

          const session = yield* Effect.promise(() =>
            createAgentFileSystem({
              server: host.url,
              folders: ["docs"],
              writable: ["docs"],
              token,
              fetch: (request) => {
                if (request.method !== "GET") writes++;

                return fetch(request);
              },
            }),
          );

          const result = yield* Effect.promise(() =>
            session.tools.write("/docs/a.txt", new TextEncoder().encode("worker agent"), {
              precondition: { kind: "any" },
              createParents: false,
            }),
          );

          if (token === "agent-token") {
            expect(result.ok).toBe(true);

            const read = yield* Effect.promise(() =>
              fetch(`${host.url}/api/folders/docs/files/a.txt`),
            );

            expect(yield* Effect.promise(() => read.text())).toBe("worker agent");
          } else {
            expect(result).toMatchObject({
              ok: false,
              error: { reason: "permission-denied", cause: { code: "EACCES" } },
            });
          }

          expect(writes).toBe(1);
        }
      }),
    { timeout: 15000 },
  );
});
