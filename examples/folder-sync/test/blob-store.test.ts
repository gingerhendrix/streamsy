import { expect, layer } from "@streamsy/effect-test";
import { Effect } from "effect";
import { BlobStore } from "../server/blob-store.ts";
import { memoryHost, sqliteHost } from "../server/storage.ts";

for (const [name, host] of [
  ["memory", memoryHost],
  ["sqlite", sqliteHost(":memory:")],
] as const) {
  layer(host)(name, (it) => {
    it.effect("blob store addresses immutable bytes by SHA-256 and deduplicates content", () =>
      Effect.gen(function* () {
        const blobs = yield* BlobStore;
        const bytes = new Uint8Array([0, 255, 1]);
        const hash = yield* blobs.put(bytes);
        expect(hash).toHaveLength(64);
        expect(yield* blobs.put(bytes)).toBe(hash);
        bytes.fill(7);
        expect(yield* blobs.get(hash)).toEqual(new Uint8Array([0, 255, 1]));
        const copy = yield* blobs.get(hash);
        copy?.fill(9);
        expect(yield* blobs.get(hash)).toEqual(new Uint8Array([0, 255, 1]));
        expect(yield* blobs.get("0".repeat(64))).toBeUndefined();
      }),
    );
  });
}
