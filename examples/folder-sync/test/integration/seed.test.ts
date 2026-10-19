import { BunCrypto, BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { spawnServer } from "@streamsy/test-support/server";
import { Effect, FileSystem, Layer, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";
import { seedDocs } from "../../scripts/seed.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { FileOp, Version } from "../../server/domain.ts";

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
      });

      const first = yield* seedDocs(server.url, `${directory}/docs`);
      const second = yield* seedDocs(server.url, `${directory}/docs`);
      expect(second).toEqual(first);

      for (const url of ["docs%2Froot/history/index.mdx", "docs%2Fstreams/history/read.mdx"]) {
        const response = yield* HttpClient.get(`${server.url}/api/folders/${url}`);

        const rows = yield* Schema.decodeUnknownEffect(
          Schema.Array(Schema.Struct({ version: Version, operation: FileOp })),
        )(yield* response.json);

        expect(rows).toHaveLength(1);
      }

      yield* server.stop;
    }),
  );
});
