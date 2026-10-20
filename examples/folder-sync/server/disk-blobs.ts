import { BunServices } from "@effect/platform-bun";
import { Effect, FileSystem, Layer, Path, Stream, Option } from "effect";
import { Predicate } from "effect";
import { BlobFailure, BlobStore } from "./blob-store.ts";
import { FILE_LIMIT, FileTooLarge, InvalidFile } from "./domain.ts";

const failure = (cause: unknown) => new BlobFailure({ message: String(cause) });

/** Bodies are hashed as they are written. A failed upload never becomes addressable. */
export const diskBlobs = (directory: string) =>
  Layer.effect(
    BlobStore,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.mapError(failure));

      const head = Effect.fn("BlobStore.disk.head")(function* (hash: string) {
        return yield* fs.stat(path.join(directory, hash)).pipe(
          Effect.map((info) => Option.some(Number(info.size))),
          Effect.catchIf(
            (error) => Predicate.isTagged(error.reason, "NotFound"),
            () => Effect.succeed(Option.none<number>()),
          ),
          Effect.map(Option.getOrUndefined),
          Effect.mapError(failure),
        );
      });

      return BlobStore.of({
        head,
        get: Effect.fn("BlobStore.disk.get")(function* (hash) {
          const size = yield* head(hash);

          return size === undefined
            ? undefined
            : { size, body: fs.stream(path.join(directory, hash)).pipe(Stream.mapError(failure)) };
        }),
        put: Effect.fn("BlobStore.disk.put")(function* (hash, body) {
          if ((yield* head(hash)) !== undefined) return undefined;

          return yield* Effect.scoped(
            Effect.gen(function* () {
              const temporary = yield* fs
                .makeTempFileScoped({ directory, prefix: ".upload-" })
                .pipe(Effect.mapError(failure));

              const output = yield* fs
                .open(temporary, { flag: "w" })
                .pipe(Effect.mapError(failure));

              const digest = new Bun.CryptoHasher("sha256");
              let size = 0;
              yield* body.pipe(
                Stream.runForEach((chunk): Effect.Effect<void, BlobFailure | FileTooLarge> => {
                  size += chunk.length;

                  if (size > FILE_LIMIT) return Effect.fail(new FileTooLarge({ size }));
                  digest.update(chunk);

                  return output.writeAll(chunk).pipe(Effect.mapError(failure));
                }),
              );

              if (digest.digest("hex") !== hash)
                return yield* new InvalidFile({ message: "Blob hash mismatch" });
              yield* output.sync.pipe(Effect.mapError(failure));
              yield* fs
                .rename(temporary, path.join(directory, hash))
                .pipe(Effect.mapError(failure));
              const parent = yield* fs
                .open(directory, { flag: "r" })
                .pipe(Effect.mapError(failure));
              yield* parent.sync.pipe(Effect.mapError(failure));

              return undefined;
            }),
          );
        }),
      });
    }),
  ).pipe(Layer.provide(BunServices.layer));
