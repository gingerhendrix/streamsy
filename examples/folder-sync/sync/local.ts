// oxlint-disable-next-line effecttsgo/node-builtin-import -- Effect FileSystem.stat follows symlinks; lstat is needed before reading or replacing a mirror path.
import { lstat } from "node:fs/promises";
import { Effect, FileSystem, Path, Predicate, Schema, Stream } from "effect";
import { FILE_LIMIT } from "../server/domain.ts";
import { ignoredPath } from "./decisions.ts";

export class LocalFailure extends Schema.TaggedError<LocalFailure>()("LocalFailure", {
  message: Schema.String,
}) {}

export class SkippedLocal extends Schema.TaggedError<SkippedLocal>()("SkippedLocal", {
  message: Schema.String,
  path: Schema.String,
}) {}

/** This adapter never follows an existing symlink within the mirror. */
export const pathKind = Effect.fn("SyncLocal.pathKind")(function* (path: string) {
  const fs = yield* FileSystem.FileSystem;

  const info = yield* Effect.tryPromise({
    try: () => lstat(path),
    catch: () => new LocalFailure({ message: `Cannot inspect ${path}` }),
  }).pipe(
    Effect.catchTag("LocalFailure", (error) =>
      fs
        .exists(path)
        .pipe(Effect.flatMap((exists) => (exists ? Effect.fail(error) : Effect.succeed(null)))),
    ),
  );

  if (info === null) return "missing";

  return info.isSymbolicLink()
    ? "symlink"
    : info.isDirectory()
      ? "directory"
      : info.isFile()
        ? "file"
        : "other";
});

export const localFiles = Effect.fn("SyncLocal.files")(function* (
  root: string,
  warn: (file: string, message: string) => Effect.Effect<void> = (_file, message) =>
    Effect.logWarning(message),
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const files: string[] = [];

  const walk = Effect.fn("SyncLocal.walk")(function* (
    relative: string,
  ): Effect.fn.Return<
    void,
    LocalFailure | Effect.Error<ReturnType<typeof fs.readDirectory>>,
    FileSystem.FileSystem
  > {
    if (ignoredPath(relative)) return yield* Effect.void;
    const absolute = path.join(root, relative);
    const kind = yield* pathKind(absolute);

    if (kind === "missing") return yield* Effect.void;

    if (kind === "symlink" || kind === "other")
      return yield* warn(absolute, `Skipping unsupported mirror path: ${absolute}`);

    if (kind === "file") {
      files.push(relative);

      return yield* Effect.void;
    }

    const names = yield* fs
      .readDirectory(absolute)
      .pipe(
        Effect.catchTag("PlatformError", (error) =>
          Predicate.isTagged("NotFound")(error.reason) ? Effect.succeed([]) : Effect.fail(error),
        ),
      );

    for (const name of names) yield* walk(path.join(relative, name));

    return yield* Effect.void;
  });

  yield* walk("");

  return files;
});

export const safePath = Effect.fn("SyncLocal.safePath")(function* (root: string, relative: string) {
  const path = yield* Path.Path;
  const parts = relative.split("/");
  let current = root;

  if (yield* pathKind(current).pipe(Effect.map((kind) => kind !== "directory")))
    return yield* new LocalFailure({ message: `Mirror root must be a real directory: ${root}` });

  for (const part of parts) {
    current = path.join(current, part);

    if ((yield* pathKind(current)) === "symlink")
      return yield* new SkippedLocal({
        path: current,
        message: `Skipping symlink in mirror path: ${current}`,
      });
  }

  return current;
});

/** Each daemon keeps its own warning signatures. lstat also fingerprints dangling symlinks. */
export const skipWarnings = Effect.sync(() => {
  const warned = new Map<string, string>();

  return (file: string, message: string) =>
    Effect.gen(function* () {
      const signature = yield* Effect.tryPromise({
        try: () =>
          lstat(file).then(
            (info) => `${info.ino}/${info.size}/${info.mtimeMs}/${info.ctimeMs}/${info.mode}`,
          ),
        catch: () => new LocalFailure({ message: `Cannot inspect ${file}` }),
      }).pipe(Effect.catchTag("LocalFailure", () => Effect.succeed("missing")));

      if (warned.get(file) === signature) return;
      warned.set(file, signature);
      yield* Effect.logWarning(message);
    });
});

/** Cache immutable observations before enqueuing. Large sources never enter SQLite or a full buffer. */
export const cacheBody = Effect.fn("SyncLocal.cacheBody")(function* <E>(
  root: string,
  body: Stream.Stream<Uint8Array, E>,
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* safePath(root, ".folder-sync/content");
  yield* fs.makeDirectory(directory, { recursive: true });

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const temporary = yield* fs.makeTempFileScoped({ directory, prefix: ".capture-" });
      const output = yield* fs.open(temporary, { flag: "w" });
      const digest = new Bun.CryptoHasher("sha256");
      let size = 0;
      yield* body.pipe(
        Stream.runForEach(
          (
            chunk,
          ): Effect.Effect<
            void,
            SkippedLocal | Effect.Error<ReturnType<typeof output.writeAll>>
          > => {
            size += chunk.length;

            if (size > FILE_LIMIT)
              return Effect.fail(
                new SkippedLocal({ path: temporary, message: "Skipping file over 64 MiB" }),
              );
            digest.update(chunk);

            return output.writeAll(chunk);
          },
        ),
      );
      const hash = digest.digest("hex");
      const source = yield* safePath(root, `.folder-sync/content/${hash}`);

      if (!(yield* fs.exists(source))) yield* fs.rename(temporary, source);

      return { hash, size, source };
    }),
  );
});

export const captureLocal = Effect.fn("SyncLocal.capture")(
  function* (root: string, relative: string) {
    const fs = yield* FileSystem.FileSystem;
    const absolute = yield* safePath(root, relative);

    if ((yield* pathKind(absolute)) !== "file") return null;
    const info = yield* fs.stat(absolute);

    if (info.size > BigInt(FILE_LIMIT))
      return yield* new SkippedLocal({
        path: absolute,
        message: `Skipping file over 64 MiB: ${absolute}`,
      });

    const digest = new Bun.CryptoHasher("sha256");
    let size = 0;
    yield* fs.stream(absolute).pipe(
      Stream.runForEach((chunk) => {
        size += chunk.length;

        if (size > FILE_LIMIT)
          return Effect.fail(
            new SkippedLocal({ path: absolute, message: `Skipping file over 64 MiB: ${absolute}` }),
          );
        digest.update(chunk);

        return Effect.void;
      }),
    );
    const hash = digest.digest("hex");
    const source = yield* safePath(root, `.folder-sync/content/${hash}`);

    if (yield* fs.exists(source)) return { hash, size, source };

    // Capture again only for new content. The capture hashes its own bytes, including any intervening edit.
    return yield* cacheBody(root, fs.stream(absolute));
  },
  (effect) =>
    effect.pipe(
      Effect.catchTag("PlatformError", (error) =>
        Predicate.isTagged(error.reason, "NotFound") ? Effect.succeed(null) : Effect.fail(error),
      ),
    ),
);

export const replaceSource = Effect.fn("SyncLocal.replaceSource")(function* (
  root: string,
  relative: string,
  source: string | null,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const absolute = yield* safePath(root, relative);

  if (source === null) {
    if ((yield* pathKind(absolute)) === "file") yield* fs.remove(absolute, { force: true });

    return;
  }

  yield* fs.makeDirectory(path.dirname(absolute), { recursive: true });
  yield* Effect.scoped(
    Effect.gen(function* () {
      const temporary = yield* fs.makeTempFileScoped({
        directory: path.join(root, ".folder-sync"),
        prefix: "pull-",
      });

      yield* fs.copyFile(source, temporary);
      yield* fs.rename(temporary, absolute);
    }),
  );
});
