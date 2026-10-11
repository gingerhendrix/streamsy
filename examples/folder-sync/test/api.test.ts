import { BunHttpServer } from "@effect/platform-bun";
import { afterAll, expect, it } from "@streamsy/effect-test";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive } from "../server/api.ts";
import { FileOp, Version } from "../server/domain.ts";
import { FolderStoreLive, Initialise, NoWriteHook } from "../server/folder-store.ts";
import { memoryHost } from "../server/storage.ts";

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
