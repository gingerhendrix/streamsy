import { Streamsy } from "@streamsy/core";
import { Crypto, Effect, Layer } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { BlobFailure, BlobStore } from "../server/blob-store.ts";
import { sha256 } from "../server/domain.ts";
import { streams } from "../server/streams.ts";
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

export const r2Blobs = (env: Env) =>
  Layer.effect(
    BlobStore,
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;

      return BlobStore.of({
        put: Effect.fn("BlobStore.R2.put")(function* (bytes) {
          const hash = yield* sha256(bytes).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError(() => new BlobFailure({ message: "Cannot hash blob" })),
          );

          yield* Effect.tryPromise({
            try: () => env.BLOBS.put(hash, bytes),
            catch: () => new BlobFailure({ message: "Cannot save blob" }),
          });

          return hash;
        }),
        get: Effect.fn("BlobStore.R2.get")(function* (hash) {
          const blob = yield* Effect.tryPromise({
            try: () => env.BLOBS.get(hash),
            catch: () => new BlobFailure({ message: "Cannot read blob" }),
          });

          if (blob === null) return undefined;

          return yield* Effect.tryPromise({
            try: () => blob.arrayBuffer().then((bytes) => new Uint8Array(bytes)),
            catch: () => new BlobFailure({ message: "Cannot read blob bytes" }),
          });
        }),
      });
    }),
  );

export const objectStorage = (
  state: DurableObjectState,
  env: Env,
  group: Streamsy.Group = streams,
) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer, r2Blobs(env)).pipe(
    Layer.provideMerge(DurableObjectStorage.layer(group, { client: { storage: state.storage } })),
    Layer.provideMerge(webCrypto),
  );
