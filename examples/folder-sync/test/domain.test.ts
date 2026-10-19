import { BunCrypto } from "@effect/platform-bun";
import { it as baseIt, layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Result, Schema } from "effect";
import {
  FILE_LIMIT,
  INLINE_LIMIT,
  FileOp,
  FileTooLarge,
  InvalidFile,
  foldHead,
  headAt,
  normalisePath,
  operationAt,
  overwriteMark,
  prepareContent,
  sha256,
} from "../server/domain.ts";

const v1 = 1;

const v2 = 2;

const first: FileOp = {
  type: "put",
  version: v1,
  path: "docs/a.md",
  hash: "a".repeat(64),
  size: 1,
  inline: "a",
  base: null,
  opId: "first",
  author: "writer",
  origin: "test",
};

baseIt.effect(
  "head fold retains tombstones and follows commit order without mutating prior heads",
  () =>
    Effect.sync(() => {
      const before = foldHead({ seq: 0, heads: {}, ops: {} }, first);

      const deletion: FileOp = {
        type: "delete",
        version: v2,
        path: first.path,
        base: v1,
        opId: "delete",
        author: "writer",
        origin: "test",
      };

      const after = foldHead(before, deletion);
      expect(before.heads[first.path]).toMatchObject({
        version: v1,
        hash: first.hash,
        size: 1,
        deleted: false,
      });
      expect(after.heads[first.path]).toMatchObject({
        version: v2,
        size: 0,
        deleted: true,
        opId: "delete",
      });
      expect(after.heads[first.path]?.hash).toBeUndefined();
      expect(overwriteMark(before.heads[first.path], v1)).toEqual({});
      expect(overwriteMark(after.heads[first.path], v1)).toEqual({ overwrote: v2 });
      expect(overwriteMark(after.heads[first.path], null)).toEqual({ overwrote: v2 });
      expect(overwriteMark(undefined, null)).toEqual({});
      expect(operationAt(after.ops, "first")).toEqual({ path: first.path, version: v1 });
      expect(operationAt(after.ops, "delete")).toEqual({ path: first.path, version: v2 });
      expect(operationAt({}, "constructor")).toBeUndefined();
      expect(headAt({}, "constructor")).toBeUndefined();
      expect(
        headAt(
          foldHead({ seq: 0, heads: {}, ops: {} }, { ...first, path: "__proto__" }).heads,
          "__proto__",
        )?.opId,
      ).toBe("first");
    }),
);

baseIt.effect(
  "path normalization removes redundant separators and dots but rejects traversal",
  () =>
    Effect.gen(function* () {
      expect(yield* normalisePath("docs//./hello world.md")).toBe("docs/hello world.md");
      expect(yield* normalisePath("雪/%2F.md")).toBe("雪/%2F.md");

      for (const path of ["", ".", "..", "/absolute", "a/../b", "a/", "a\\b", "a\0b", "a\nb"]) {
        const failure = yield* Effect.flip(normalisePath(path));
        expect(failure).toBeInstanceOf(InvalidFile);
      }
    }),
);

baseIt.effect("FileOp schema rejects missing content, invalid hashes and malformed bases", () =>
  Effect.gen(function* () {
    for (const invalid of [
      { ...first, inline: undefined },
      { ...first, hash: "not a hash" },
      { ...first, base: "1" },
      { ...first, version: undefined },
      { ...first, version: "1" },
      { ...first, version: 0 },
      { ...first, version: -1 },
      { ...first, version: 1.5 },
      { ...first, version: Number.MAX_SAFE_INTEGER + 1 },
      { ...first, size: -1 },
      { ...first, opId: "" },
      { ...first, path: "a/../b" },
      { ...first, size: 2 },
      { ...first, inline: "x".repeat(INLINE_LIMIT + 1), size: INLINE_LIMIT + 1 },
    ]) {
      expect(
        Result.isFailure(yield* Schema.decodeUnknownEffect(FileOp)(invalid).pipe(Effect.result)),
      ).toBe(true);
    }
  }),
);

layer(BunCrypto.layer)((it) => {
  it.effect("SHA-256 and inline byte limits preserve UTF-8, including BOMs", () =>
    Effect.gen(function* () {
      expect(yield* sha256(new TextEncoder().encode("abc"))).toBe(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
      );
      const exact = yield* prepareContent(new TextEncoder().encode("x".repeat(INLINE_LIMIT)));
      expect("inline" in exact).toBe(true);
      const larger = yield* prepareContent(new TextEncoder().encode("x".repeat(INLINE_LIMIT + 1)));
      expect("blob" in larger).toBe(true);
      const utf8 = new TextEncoder().encode("雪".repeat(INLINE_LIMIT / 3 + 1));
      expect("blob" in (yield* prepareContent(utf8))).toBe(true);
      const bom = new Uint8Array([239, 187, 191, 97]);
      const content = yield* prepareContent(bom);

      if (!("inline" in content)) throw new Error("Expected BOM inline");
      expect(new TextEncoder().encode(content.inline)).toEqual(bom);
      expect("inline" in (yield* prepareContent(new Uint8Array()))).toBe(true);
    }),
  );
  it.effect("binary uses blobs and the 8 MiB file limit is inclusive", () =>
    Effect.gen(function* () {
      expect("blob" in (yield* prepareContent(new Uint8Array([0, 255])))).toBe(true);
      expect("blob" in (yield* prepareContent(new Uint8Array([255])))).toBe(true);
      expect((yield* prepareContent(new Uint8Array(FILE_LIMIT))).size).toBe(FILE_LIMIT);
      expect(yield* Effect.flip(prepareContent(new Uint8Array(FILE_LIMIT + 1)))).toBeInstanceOf(
        FileTooLarge,
      );
    }),
  );
});
