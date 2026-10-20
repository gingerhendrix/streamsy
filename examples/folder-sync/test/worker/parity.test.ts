/* oxlint-disable effecttsgo/async-function -- Compare native HTTP responses from both local host adapters. */
import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { CreateFolder, CommitFile, DeleteFile } from "../../server/domain.ts";
import { expect } from "vitest";
import { startWorker } from "../../scripts/worker-harness.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { protocolHost, writerToken } from "../protocol-host.ts";

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "Bun and Worker agree on write, error, stream and blob responses",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("host-parity-");

        const bun = yield* Effect.acquireRelease(Effect.sync(protocolHost), (host) =>
          Effect.promise(host.stop),
        );

        const worker = yield* Effect.acquireRelease(
          Effect.promise(() => startWorker(directory, writerToken)),
          (host) => Effect.promise(host.stop),
        );

        const bytes = new Uint8Array([0, 1, 255]);
        const hash = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");

        const create = yield* Schema.encodeEffect(Schema.fromJsonString(CreateFolder))({
          txid: "create",
          folderId: "docs",
          title: "Docs",
        });

        const commit = yield* Schema.encodeEffect(Schema.fromJsonString(CommitFile))({
          txid: "binary",
          author: "test",
          hash,
          size: bytes.length,
        });

        const deletion = yield* Schema.encodeEffect(Schema.fromJsonString(DeleteFile))({
          txid: "delete",
          author: "test",
        });

        yield* Effect.promise(async () => {
          const authorization = { authorization: `Bearer ${writerToken}` };
          const jsonHeaders = { ...authorization, "content-type": "application/json" };

          // oxlint-disable-next-line unicorn/consistent-function-scoping -- Comparison closes over this fixture's two scoped hosts.
          const compare = async (path: string, status: number, init: RequestInit = {}) => {
            const a = await bun.fetch(new Request(`http://localhost${path}`, init));

            // oxlint-disable-next-line effecttsgo/global-fetch -- Real local Worker transport is the boundary under test.
            const b = await fetch(`${worker.url}${path}`, {
              ...init,
              headers: {
                ...Object.fromEntries(new Headers(init.headers)),
                "accept-encoding": "identity",
              },
            });

            expect(a.status, `Bun ${path}`).toBe(status);
            expect(b.status, `Worker ${path}`).toBe(status);
            const text = await a.text();
            expect(await b.text(), path).toBe(text);

            for (const name of ["etag", "allow", "cache-control", "content-type", "content-length"])
              if (name !== "content-length" || text.length > 0)
                expect(b.headers.get(name), `${path} ${name}`).toBe(a.headers.get(name));

            if (["HEAD", "GET"].includes(init.method ?? "GET") && path.includes("/blobs/"))
              expect(b.headers.get("content-length"), `${path} length`).toBe(
                a.headers.get("content-length"),
              );
          };

          await compare("/api/folders", 401, { method: "POST", body: create });
          await compare("/api/folders", 415, {
            method: "POST",
            headers: { ...authorization, "content-type": "text/plain" },
            body: create,
          });
          await compare("/api/folders", 200, {
            method: "POST",
            headers: jsonHeaders,
            body: create,
          });

          for (const method of ["POST", "PUT", "DELETE", "OPTIONS"]) {
            for (const path of [
              "/streams/folders",
              "/streams/folders/646f6373/files",
              "/streams/other",
            ])
              await compare(path, 405, { method });
          }

          await compare("/streams/other", 404);
          await compare("/streams/folders/nothex/files", 400);
          await compare("/streams/folders/f/files", 400);
          await compare("/api/folders/%E0%A4%A/files/a", 404, {
            method: "PUT",
            headers: jsonHeaders,
            body: commit,
          });
          await compare(`/api/folders/${"a".repeat(101)}/files/a`, 400, {
            method: "PUT",
            headers: jsonHeaders,
            body: commit,
          });

          for (let repeat = 0; repeat < 2; repeat++)
            await compare(`/api/blobs/${hash}`, 200, {
              method: "PUT",
              headers: { ...authorization, "content-length": String(bytes.length) },
              body: bytes,
            });
          await compare(`/api/blobs/${hash}`, 200, { method: "HEAD" });
          await compare(`/api/blobs/${hash}`, 200);
          await compare("/api/blobs/bad", 400);
          await compare(`/api/blobs/${hash}`, 405, { method: "POST", headers: authorization });
          await compare("/api/folders/docs/files/binary", 200, {
            method: "PUT",
            headers: jsonHeaders,
            body: commit,
          });
          await compare("/api/folders/docs/files/binary", 200);
          await compare("/api/folders/docs/files/binary", 412, {
            method: "DELETE",
            headers: { ...jsonHeaders, "if-match": '"99"' },
            body: deletion,
          });
        });
      }),
    { timeout: 20000 },
  );
});
