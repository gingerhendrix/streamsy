// oxlint-disable-next-line effecttsgo/node-builtin-import -- Effect FileSystem.stat follows symlinks; lstat is needed before reading or replacing a mirror path.
import { lstat } from "node:fs/promises";
import { Effect, FileSystem, Path, Predicate, Schema } from "effect";
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

export const localBytes = Effect.fn("SyncLocal.read")(
  function* (root: string, relative: string) {
    const fs = yield* FileSystem.FileSystem;
    const absolute = yield* safePath(root, relative);

    if (!(yield* fs.exists(absolute))) return null;
    const info = yield* fs.stat(absolute);

    if (info.type !== "File") return null;

    if (info.size > BigInt(FILE_LIMIT))
      return yield* new SkippedLocal({
        path: absolute,
        message: `Skipping file over 8 MiB: ${absolute}`,
      });

    return yield* fs.readFile(absolute);
  },
  (effect) =>
    effect.pipe(
      Effect.catchTag("PlatformError", (error) =>
        Predicate.isTagged("NotFound")(error.reason) ? Effect.succeed(null) : Effect.fail(error),
      ),
    ),
);

/** Atomic replacement keeps watchers from observing partly written content. */
export const replaceLocal = Effect.fn("SyncLocal.replace")(function* (
  root: string,
  relative: string,
  bytes: Uint8Array | null,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const absolute = yield* safePath(root, relative);

  if (bytes === null) {
    if ((yield* pathKind(absolute)) === "file") yield* fs.remove(absolute, { force: true });

    return;
  }

  yield* fs.makeDirectory(path.dirname(absolute), { recursive: true });

  // Scratch stays under the ignored state directory, on the same filesystem as the mirror.
  const temp = yield* fs.makeTempFileScoped({
    directory: path.join(root, ".folder-sync"),
    prefix: "pull-",
  });

  yield* fs.writeFile(temp, bytes);
  yield* fs.rename(temp, absolute);
}, Effect.scoped);

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
