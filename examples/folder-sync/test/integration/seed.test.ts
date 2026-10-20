import { BunCrypto, BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { spawnServer } from "@streamsy/test-support/server";
import { Effect, FileSystem, Layer, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { seedDocs } from "../../scripts/seed.ts";
import { FolderKey } from "../../server/streams.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { File } from "../../server/domain.ts";

layer(Layer.mergeAll(BunServices.layer, BunCrypto.layer, FetchHttpClient.layer), {
  excludeTestServices: true,
})((it) => {
  it.effect("seeding twice through HTTP adds no versions", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("seed-");
      const fs = yield* FileSystem.FileSystem;
      yield* fs.makeDirectory(`${directory}/docs/streams`, { recursive: true });
      yield* fs.writeFileString(`${directory}/docs/index.mdx`, "# Docs");
      yield* fs.writeFileString(`${directory}/docs/streams/read.mdx`, "# Read");

      const server = yield* spawnServer({
        cwd: `${import.meta.dirname}/../..`,
        entry: "server/index.ts",
        env: { FOLDER_SYNC_WRITER_TOKEN: "seed-test-token" },
      });

      const client = yield* HttpClient.HttpClient;

      const authenticated = HttpClient.mapRequest(
        client,
        HttpClientRequest.bearerToken("seed-test-token"),
      );

      const first = yield* seedDocs(server.url, `${directory}/docs`).pipe(
        Effect.provideService(HttpClient.HttpClient, authenticated),
      );

      const second = yield* seedDocs(server.url, `${directory}/docs`).pipe(
        Effect.provideService(HttpClient.HttpClient, authenticated),
      );

      expect(second).toEqual(first);

      for (const folder of ["docs/root", "docs/streams"]) {
        const response = yield* HttpClient.get(
          `${server.url}/streams/folders/${yield* Schema.encodeEffect(FolderKey)(folder)}/files?offset=-1`,
        );

        const rows = yield* Schema.decodeUnknownEffect(
          Schema.Array(Schema.Struct({ value: File })),
        )(yield* response.json);

        expect(rows).toHaveLength(1);
      }

      yield* server.stop;
    }),
  );
});
