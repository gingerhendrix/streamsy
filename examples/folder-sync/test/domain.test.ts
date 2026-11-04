import { Digest } from "effect/crypto";
import { BunCrypto } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect } from "effect";
import {
  FILE_LIMIT,
  FileTooLarge,
  InvalidFile,
  normalisePath,
  prepareContent,
  sha256,
} from "../server/domain.ts";

layer(BunCrypto.layer)((it) => {
  it.effect("content is named by hash and size, with no inline text", () =>
    Effect.gen(function* () {
      expect(yield* sha256(new TextEncoder().encode("abc"))).toBe(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
      );
      expect(yield* prepareContent(new TextEncoder().encode("abc"))).toEqual({
        hash: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        size: 3,
      });
      expect(yield* Effect.flip(prepareContent(new Uint8Array(FILE_LIMIT + 1)))).toBeInstanceOf(
        FileTooLarge,
      );
    }),
  );
  it.effect("paths reject traversal and preserve opaque segments", () =>
    Effect.gen(function* () {
      expect(yield* normalisePath("a//./雪%2F")).toBe("a/雪%2F");

      for (const path of ["", "/a", "a/../b", "a/", "a\\b", "a\0b"])
        expect(yield* Effect.flip(normalisePath(path))).toBeInstanceOf(InvalidFile);
    }),
  );
});

for (const [name, crypto] of [
  ["Bun", BunCrypto.layerDigest],
  ["WebCrypto", Digest.layerWebCrypto],
] as const) {
  layer(crypto)(`${name} content hash compatibility`, (it) => {
    it.effect("preserves SHA-256 bytes for empty, binary and Unicode content", () =>
      Effect.gen(function* () {
        const digest = yield* Digest.Digest;

        for (const bytes of [
          new Uint8Array(),
          new Uint8Array([0, 255, 128, 0, 1]),
          new TextEncoder().encode("雪\nStreamsy"),
          new Uint8Array(65_537).fill(255),
        ]) {
          const expected = new Uint8Array(new Bun.CryptoHasher("sha256").update(bytes).digest());
          expect(yield* digest.digest(Digest.sha256, bytes)).toEqual(expected);
          expect(yield* sha256(bytes)).toBe(
            new Bun.CryptoHasher("sha256").update(bytes).digest("hex"),
          );
        }
      }),
    );
  });
}
