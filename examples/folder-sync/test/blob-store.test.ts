import { BunServices, BunCrypto } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, FileSystem, Layer, Stream } from "effect";
import { BlobStore, memoryBlobs } from "../server/blob-store.ts";
import { diskBlobs } from "../server/disk-blobs.ts";
import { FileTooLarge, InvalidFile, sha256 } from "../server/domain.ts";
import { testDirectory } from "../scripts/test-directory.ts";

const disk = Layer.unwrap(testDirectory("blobs-").pipe(Effect.map(diskBlobs))).pipe(
  Layer.provide(BunServices.layer),
);

for (const [name, host] of [
  ["memory", memoryBlobs],
  ["disk", disk],
] as const) {
  layer(host.pipe(Layer.provideMerge(BunCrypto.layer)))(name, (it) => {
    it.effect("hash verification, immutable streaming reads, HEAD and no-write repeats", () =>
      Effect.gen(function* () {
        const blobs = yield* BlobStore;
        const bytes = new Uint8Array([0, 255, 1]);
        const hash = yield* sha256(bytes);
        expect(yield* blobs.head(hash)).toBeUndefined();
        expect(
          yield* Effect.flip(blobs.put(hash, Stream.succeed(new Uint8Array([1])))),
        ).toBeInstanceOf(InvalidFile);
        expect(yield* blobs.head(hash)).toBeUndefined();
        yield* blobs.put(hash, Stream.succeed(bytes));
        expect(yield* blobs.head(hash)).toBe(3);
        yield* blobs.put(hash, Stream.die("repeat body must never be pulled"));
        const content = yield* blobs.get(hash);

        if (content === undefined) throw new Error("Missing stored blob");
        const chunks = yield* Stream.runCollect(content.body);
        expect(Array.from(chunks).flatMap((chunk) => [...chunk])).toEqual([0, 255, 1]);
        expect(yield* blobs.get("0".repeat(64))).toBeUndefined();
      }),
    );
    it.effect("streamed bodies enforce 64 MiB even without Content-Length", () =>
      Effect.gen(function* () {
        const blobs = yield* BlobStore;
        const chunk = new Uint8Array(1024 * 1024);
        const oversized = Stream.fromIterable(Array.from({ length: 65 }, () => chunk));
        expect(yield* Effect.flip(blobs.put("a".repeat(64), oversized))).toBeInstanceOf(
          FileTooLarge,
        );
        expect(yield* blobs.head("a".repeat(64))).toBeUndefined();
      }),
    );
  });
}

layer(BunServices.layer)((it) => {
  it.effect("failed disk uploads remove temporary files and restart reopens complete blobs", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("blob-restart-");
      const fs = yield* FileSystem.FileSystem;
      const hash = "0".repeat(64);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(diskBlobs(directory));
          yield* Effect.gen(function* () {
            const blobs = yield* BlobStore;
            yield* Effect.flip(blobs.put(hash, Stream.succeed(new Uint8Array([1]))));
          }).pipe(Effect.provideContext(context));
        }),
      );
      expect(yield* fs.readDirectory(directory)).toEqual([]);
    }),
  );
});
