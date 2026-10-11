import { BunCrypto, BunRuntime, BunServices } from "@effect/platform-bun";
import { Console, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { sha256, Version } from "../server/domain.ts";
import { encodeFilePath } from "../sync/decisions.ts";

class SeedFailure extends Schema.TaggedError<SeedFailure>()("SeedFailure", {
  message: Schema.String,
}) {}

const Written = Schema.Struct({ version: Version });

/** The first directory is the docs section. Root files have their own disjoint folder. */
export const seedLocation = (relative: string) => {
  const parts = relative.split("/");

  return parts.length === 1
    ? { folderId: "docs/root", path: relative }
    : { folderId: `docs/${parts[0]}`, path: parts.slice(1).join("/") };
};

export const seedDocs = Effect.fn("FolderSync.seedDocs")(function* (
  server: string,
  directory = `${import.meta.dir}/../../../site/content/docs`,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = server.replace(/\/$/, "");
  const created = new Set<string>();
  const versions = new Map<string, string>();
  const files = (yield* fs.readDirectory(directory, { recursive: true })).toSorted();

  for (const relative of files) {
    const absolute = path.join(directory, relative);

    if ((yield* fs.stat(absolute)).type !== "File") continue;
    const location = seedLocation(relative);

    if (!created.has(location.folderId)) {
      const response = yield* HttpClient.post(`${root}/api/folders`, {
        body: HttpBody.jsonUnsafe({
          folderId: location.folderId,
          title: `Streamsy ${location.folderId.slice(5)}`,
        }),
      });

      if (response.status !== 200)
        return yield* new SeedFailure({ message: `Folder creation answered ${response.status}` });
      created.add(location.folderId);
    }

    const bytes = yield* fs.readFile(absolute);
    const hash = yield* sha256(bytes);
    const url = `${root}/api/folders/${encodeURIComponent(location.folderId)}/files/${encodeFilePath(location.path)}`;
    const head = yield* HttpClient.get(url);

    if (head.status !== 200 && head.status !== 404)
      return yield* new SeedFailure({ message: `Head read answered ${head.status}` });
    const base = head.headers["x-file-version"];

    const key = yield* Schema.encodeEffect(
      Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String, Schema.String])),
    )([location.folderId, location.path, hash]);

    const headers = {
      "x-file-author": encodeURIComponent("docs seed"),
      "x-file-origin": encodeURIComponent("seed"),
      "x-file-op-id": encodeURIComponent(`seed:${key}`),
    };

    const response = yield* HttpClient.put(url, {
      headers: base === undefined ? headers : { ...headers, "x-file-base": base },
      body: HttpBody.uint8Array(bytes),
    });

    if (response.status !== 200)
      return yield* new SeedFailure({ message: `${relative}: write answered ${response.status}` });
    const receipt = yield* Schema.decodeUnknownEffect(Written)(yield* response.json);
    versions.set(relative, receipt.version);
  }

  yield* Console.log(
    `Seeded ${versions.size} files in ${created.size} folders through the HTTP API`,
  );

  return versions;
});

if (import.meta.main) {
  const args = Bun.argv.slice(2);

  const server =
    args.length === 0
      ? "http://localhost:1339"
      : args.length === 2 && args[0] === "--server"
        ? args[1]
        : undefined;

  BunRuntime.runMain(
    (server === undefined
      ? Effect.fail(new SeedFailure({ message: "bun run seed [--server <url>]" }))
      : seedDocs(server)
    ).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- The seed executable owns the HTTP and Bun runtime.
      Effect.provide(Layer.mergeAll(BunServices.layer, BunCrypto.layer, FetchHttpClient.layer)),
    ),
  );
}
