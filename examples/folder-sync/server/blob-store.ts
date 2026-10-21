import { Context, Crypto, Effect, Layer, Schema, Stream } from "effect";
import { collectBytes } from "./bytes.ts";
import { FILE_LIMIT, FileTooLarge, InvalidFile, sha256 } from "./domain.ts";

export class BlobFailure extends Schema.TaggedError<
  BlobFailure,
  { readonly brand: unique symbol }
>()("BlobFailure", {
  message: Schema.String,
}) {}

export interface BlobContent {
  readonly size: number;
  readonly body: Stream.Stream<Uint8Array, BlobFailure>;
}

export class BlobStore extends Context.Service<
  BlobStore,
  {
    readonly put: (
      hash: string,
      body: Stream.Stream<Uint8Array, BlobFailure>,
    ) => Effect.Effect<void, BlobFailure | InvalidFile | FileTooLarge>;
    readonly head: (hash: string) => Effect.Effect<number | undefined, BlobFailure>;
    readonly get: (hash: string) => Effect.Effect<BlobContent | undefined, BlobFailure>;
  }
>()("FolderSync/BlobStore") {}

/** Tests only. Production stores consume bodies without accumulating them. */
export const memoryBlobs = Layer.effect(
  BlobStore,
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const blobs = new Map<string, Uint8Array>();

    return BlobStore.of({
      put: Effect.fn("BlobStore.memory.put")(function* (hash, body) {
        if (blobs.has(hash)) return undefined;
        const bytes = yield* collectBytes(body, FILE_LIMIT, (size) => new FileTooLarge({ size }));

        const actual = yield* sha256(bytes).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError(() => new BlobFailure({ message: "Cannot hash blob" })),
        );

        if (actual !== hash) return yield* new InvalidFile({ message: "Blob hash mismatch" });
        blobs.set(hash, bytes);

        return undefined;
      }),
      head: (hash) => Effect.sync(() => blobs.get(hash)?.length),
      get: (hash) =>
        Effect.sync(() => {
          const bytes = blobs.get(hash);

          return bytes === undefined
            ? undefined
            : { size: bytes.length, body: Stream.succeed(bytes.slice()) };
        }),
    });
  }),
);
