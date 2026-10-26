import { Streamsy } from "@streamsy/core";
import { Crypto, Effect, Layer, Stream } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { BlobFailure, BlobStore } from "../server/blob-store.ts";
import type { Env } from "./env.ts";

const webCrypto = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, bytes) =>
      Effect.promise(() => crypto.subtle.digest(algorithm, bytes)).pipe(
        Effect.map((buffer) => new Uint8Array(buffer)),
      ),
  }),
);

/** Folder owners only inspect and read immutable blobs. Uploads enter through the Worker API. */
export const r2Blobs = (env: Env) =>
  Layer.succeed(
    BlobStore,
    BlobStore.of({
      put: () => Effect.fail(new BlobFailure({ message: "Upload blobs through the Worker API" })),
      head: Effect.fn("BlobStore.R2.head")((hash) =>
        Effect.tryPromise({
          try: () => env.BLOBS.head(hash).then((blob) => blob?.size),
          catch: () => new BlobFailure({ message: "Cannot inspect blob" }),
        }),
      ),
      get: Effect.fn("BlobStore.R2.get")(function* (hash) {
        const blob = yield* Effect.tryPromise({
          try: () => env.BLOBS.get(hash),
          catch: () => new BlobFailure({ message: "Cannot read blob" }),
        });

        return blob === null
          ? undefined
          : {
              size: blob.size,
              body: Stream.fromReadableStream({
                evaluate: () => blob.body,
                onError: () => new BlobFailure({ message: "Cannot read blob bytes" }),
              }),
            };
      }),
    }),
  );

/** Each object owns only the streams in `group`. */
export const objectStorage = (state: DurableObjectState, env: Env, group: Streamsy.Group) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer, r2Blobs(env)).pipe(
    Layer.provideMerge(DurableObjectStorage.layer(group, { client: { storage: state.storage } })),
    Layer.provideMerge(webCrypto),
  );
