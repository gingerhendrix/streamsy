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
