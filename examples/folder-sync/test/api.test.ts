import { BunHttpServer } from "@effect/platform-bun";
import { OffsetMismatch, StorageFault, StreamId } from "@streamsy/core";
import { it } from "@effect/vitest";
import { afterAll, expect } from "vitest";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive } from "../server/api.ts";
import { FileOp, Version } from "../server/domain.ts";
import { FolderStoreLive, Initialise, NoWriteHook, WriteHook } from "../server/folder-store.ts";
import { memoryHost, sqliteHost } from "../server/storage.ts";

const web = HttpRouter.toWebHandler(
  ApiLive.pipe(
    Layer.provide(Initialise),
    Layer.provide(FolderStoreLive),
    Layer.provide(NoWriteHook),
    Layer.provide([memoryHost, BunHttpServer.layerHttpServices]),
  ),
  { disableLogger: true },
);

afterAll(() => web.dispose());

const send = (request: Request) => Effect.promise(() => web.handler(request));

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

const metadata = {
  "x-file-op-id": encodeURIComponent("op/雪%"),
  "x-file-author": encodeURIComponent("Zoë"),
  "x-file-origin": encodeURIComponent("client/雪%"),
};

const create = (folderId: string) =>
  send(
    new Request("http://localhost/api/folders", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: Schema.encodeSync(
        Schema.fromJsonString(Schema.Struct({ folderId: Schema.String, title: Schema.String })),
      )({ folderId, title: "API test" }),
    }),
  );

it.live("folder ID boundaries work after URL decoding and invalid creates answer 400", () =>
  Effect.gen(function* () {
    for (const folderId of [
      "a".repeat(100),
      "/".repeat(100),
      "%".repeat(100),
      "雪".repeat(83),
      "😀".repeat(50),
    ]) {
      expect((yield* create(folderId)).status).toBe(200);
      const url = `http://localhost/api/folders/${encodeURIComponent(folderId)}/files/a`;
      expect(
        (yield* send(new Request(url, { method: "PUT", headers: metadata, body: folderId })))
          .status,
      ).toBe(200);
      const read = yield* send(new Request(url));
      expect(read.status).toBe(200);
      expect(yield* Effect.promise(() => read.text())).toBe(folderId);
      expect(
        (yield* send(
          new Request(`http://localhost/api/folders/${encodeURIComponent(folderId)}/history/a`),
        )).status,
      ).toBe(200);
      expect(
        (yield* send(
          new Request(url, {
            method: "DELETE",
            headers: { ...metadata, "x-file-op-id": "delete" },
          }),
        )).status,
      ).toBe(200);
    }

    for (const folderId of ["a".repeat(101), "雪".repeat(84)]) {
      const rejected = yield* create(folderId);
      expect(rejected.status).toBe(400);
      expect(yield* Effect.promise(() => rejected.text())).toContain(
        "100 decoded UTF-16 code units and 249 UTF-8 bytes",
      );
    }
  }),
);

it.live("metadata headers round trip Unicode and literal percent signs on PUT and DELETE", () =>
  Effect.gen(function* () {
    yield* create("metadata");
    const url = "http://localhost/api/folders/metadata/files/a";
    const put = yield* send(new Request(url, { method: "PUT", headers: metadata, body: "text" }));
    expect(put.status).toBe(200);

    const deletion = yield* send(
      new Request(url, {
        method: "DELETE",
        headers: { ...metadata, "x-file-op-id": encodeURIComponent("delete/雪%") },
      }),
    );

    expect(deletion.status).toBe(200);
    const response = yield* send(new Request("http://localhost/api/folders/metadata/history/a"));

    const history = yield* Schema.decodeUnknownEffect(History)(
      yield* Effect.promise(() => response.json()),
    );

    expect(
      history.map(({ operation }) => ({
        author: operation.author,
        origin: operation.origin,
        opId: operation.opId,
      })),
    ).toEqual([
      { author: "Zoë", origin: "client/雪%", opId: "op/雪%" },
      { author: "Zoë", origin: "client/雪%", opId: "delete/雪%" },
    ]);
  }),
);

it.live(
  "invalid percent escapes, invalid UTF-8 and invalid decoded metadata answer 400 without writes",
  () =>
    Effect.gen(function* () {
      yield* create("invalid-metadata");
      const url = "http://localhost/api/folders/invalid-metadata/files/a";

      for (const method of ["PUT", "DELETE"]) {
        for (const header of ["x-file-op-id", "x-file-author", "x-file-origin"]) {
          for (const value of ["%", "%GG", "%C3", "%FF", "%00", ""]) {
            const request = new Request(url, { method, headers: { ...metadata, [header]: value } });
            expect((yield* send(request)).status).toBe(400);
          }
        }
      }

      const response = yield* send(
        new Request("http://localhost/api/folders/invalid-metadata/history/a"),
      );

      expect(
        yield* Schema.decodeUnknownEffect(History)(yield* Effect.promise(() => response.json())),
      ).toEqual([]);
    }),
);

for (const [host, storage] of [
  ["memory", memoryHost],
  ["sqlite", sqliteHost(":memory:")],
] as const) {
  it.live(
    `${host}: hook storage and append failures answer 500 and preserve the previous head`,
    () =>
      Effect.gen(function* () {
        for (const fault of [
          new StorageFault({
            operation: "hook.append",
            message: "disk unavailable",
            retryable: true,
          }),
          new OffsetMismatch({ id: StreamId.make("hook-events"), expected: "a", actual: "b" }),
        ]) {
          const edge = yield* Effect.acquireRelease(
            Effect.sync(() =>
              HttpRouter.toWebHandler(
                ApiLive.pipe(
                  Layer.provide(Initialise),
                  Layer.provide(FolderStoreLive),
                  Layer.provide(
                    Layer.succeed(WriteHook, {
                      committed: (_folderId, operation) =>
                        operation.opId === "failing" ? Effect.fail(fault) : Effect.void,
                    }),
                  ),
                  Layer.provide([storage, BunHttpServer.layerHttpServices]),
                ),
                { disableLogger: true },
              ),
            ),
            (server) => Effect.promise(() => server.dispose()),
          );

          const call = (request: Request) => Effect.promise(() => edge.handler(request));

          const folder = yield* call(
            new Request("http://localhost/api/folders", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: '{"folderId":"hook","title":"Hook"}',
            }),
          );

          expect(folder.status).toBe(200);
          const url = "http://localhost/api/folders/hook/files/a";

          const initial = yield* call(
            new Request(url, { method: "PUT", headers: metadata, body: "initial" }),
          );

          expect(initial.status).toBe(200);

          const failed = yield* call(
            new Request(url, {
              method: "PUT",
              headers: { ...metadata, "x-file-op-id": "failing" },
              body: "must roll back",
            }),
          );

          expect(failed.status).toBe(500);
          expect(yield* Effect.promise(() => failed.json())).toMatchObject({
            error: "Storage failure",
          });
          const head = yield* call(new Request(url));
          expect(yield* Effect.promise(() => head.text())).toBe("initial");
          const response = yield* call(new Request("http://localhost/api/folders/hook/history/a"));
          expect(
            yield* Schema.decodeUnknownEffect(History)(
              yield* Effect.promise(() => response.json()),
            ),
          ).toHaveLength(1);
        }
      }),
  );
}
