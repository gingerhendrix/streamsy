import { BunCrypto, BunRuntime, BunServices } from "@effect/platform-bun";
import {
  Array,
  Config,
  Console,
  Effect,
  FileSystem,
  Layer,
  Option,
  Order,
  Path,
  Schema,
} from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { prepareContent, Version, VersionFromString } from "../server/domain.ts";
import { fileUrl } from "../server/routes.ts";

class SeedFailure extends Schema.TaggedError<SeedFailure, { readonly brand: unique symbol }>()(
  "SeedFailure",
  {
    message: Schema.String,
  },
) {}

const Written = Schema.Struct({ version: Version });

/** The version of the last change of a path: the head, a tombstone, or `null` for no change. */
const parentOf = Effect.fn("FolderSync.seedParent")(function* (url: string) {
  const response = yield* HttpClient.get(url);

  if (response.status === 200) {
    yield* response.text;

    return yield* Schema.decodeUnknownEffect(VersionFromString)(
      response.headers.etag?.replace(/^"|"$/g, ""),
    );
  }

  if (response.status !== 404)
    return yield* new SeedFailure({ message: `${url}: read answered ${response.status}` });
  const body = yield* response.json.pipe(Effect.orElseSucceed(() => ({})));

  return Option.getOrNull(
    Option.map(
      Schema.decodeUnknownOption(Schema.Struct({ deleted: Version }))(body),
      ({ deleted }) => deleted,
    ),
  );
});

/** The first directory is the docs section. Root files have their own disjoint folder. */
export const seedLocation = (relative: string) => {
  const parts = relative.split("/");

  return parts.length === 1
    ? { folderId: "docs/root", path: relative }
    : { folderId: `docs/${parts[0]}`, path: Array.join(parts.slice(1), "/") };
};

export const seedDocs = Effect.fn("FolderSync.seedDocs")(function* (
  server: string,
  directory = `${import.meta.dir}/../../../site/content/docs`,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = server.replace(/\/$/, "");
  const created = new Set<string>();
  const versions = new Map<string, number>();
  const files = Array.sort(yield* fs.readDirectory(directory, { recursive: true }), Order.String);

  for (const relative of files) {
    const absolute = path.join(directory, relative);

    if ((yield* fs.stat(absolute)).type !== "File") continue;
    const location = seedLocation(relative);

    if (!created.has(location.folderId)) {
      const response = yield* HttpClient.post(`${root}/api/folders`, {
        body: HttpBody.jsonUnsafe({
          txid: `seed-folder:${location.folderId}`,
          folderId: location.folderId,
          title: `Streamsy ${location.folderId.slice(5)}`,
        }),
      });

      if (response.status !== 200)
        return yield* new SeedFailure({ message: `Folder creation answered ${response.status}` });
      created.add(location.folderId);
    }

    const bytes = yield* fs.readFile(absolute);
    const content = yield* prepareContent(bytes);
    const hash = content.hash;
    const url = `${root}${fileUrl(location.folderId, location.path)}`;
    // The seed writes over the current head. The parent in the txid gives a new txid
    // after a user edit, so the retry memory does not answer with an old seed write.
    const parent = yield* parentOf(url);

    const key = yield* Schema.encodeEffect(
      Schema.fromJsonString(
        Schema.Tuple([Schema.String, Schema.String, Schema.String, Schema.NullOr(Version)]),
      ),
    )([location.folderId, location.path, hash, parent]);

    const upload = yield* HttpClient.put(`${root}/api/blobs/${hash}`, {
      body: HttpBody.uint8Array(bytes),
    });

    if (upload.status !== 200)
      return yield* new SeedFailure({
        message: `${relative}: blob upload answered ${upload.status}`,
      });

    const response = yield* HttpClient.put(url, {
      body: HttpBody.jsonUnsafe({ ...content, parent, txid: `seed:${key}`, author: "docs seed" }),
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

  const clientLayer = Layer.effect(
    HttpClient.HttpClient,
    Effect.gen(function* () {
      const token = yield* Config.String("FOLDER_SYNC_WRITER_TOKEN").pipe(Config.withDefault(""));
      const client = yield* HttpClient.HttpClient;

      return token === ""
        ? client
        : HttpClient.mapRequest(client, HttpClientRequest.bearerToken(token));
    }),
  ).pipe(Layer.provide(FetchHttpClient.layer));

  BunRuntime.runMain(
    (server === undefined
      ? Effect.fail(new SeedFailure({ message: "bun run seed [--server <url>]" }))
      : seedDocs(server)
    ).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- The seed executable owns the HTTP and Bun runtime.
      Effect.provide(Layer.mergeAll(BunServices.layer, BunCrypto.layer, clientLayer)),
    ),
  );
}
