import { layer } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { expect, test } from "vitest";
import { bearerMatches } from "../server/auth.ts";
import { collectBytes, concatBytes, decodeText, hex, limitBytes } from "../server/bytes.ts";
import { hashStream } from "../server/hash-stream.ts";

class TooLarge extends Schema.TaggedError<TooLarge, { readonly brand: unique symbol }>()(
  "TooLarge",
  { size: Schema.Int },
) {}

const bytes = (...values: number[]) => new Uint8Array(values);

test("concatBytes, hex and decodeText", () => {
  expect(concatBytes([bytes(1, 2), bytes(), bytes(3)], 3)).toEqual(bytes(1, 2, 3));
  expect(hex(bytes(0, 15, 255))).toBe("000fff");
  expect(decodeText(new TextEncoder().encode("雪"))).toBe("雪");
  expect(decodeText(bytes(0))).toBeUndefined();
  expect(decodeText(bytes(255))).toBeUndefined();
  // A prefix cut inside a code point decodes with `truncated`.
  expect(decodeText(new TextEncoder().encode("a雪").subarray(0, 3))).toBeUndefined();
  expect(decodeText(new TextEncoder().encode("a雪").subarray(0, 3), true)).toBe("a");
});

test("bearerMatches needs the exact token and accepts nothing for an empty token", () => {
  expect(bearerMatches("Bearer secret", "secret")).toBe(true);
  expect(bearerMatches("Bearer secreT", "secret")).toBe(false);
  expect(bearerMatches("Bearer secret2", "secret")).toBe(false);
  expect(bearerMatches(undefined, "secret")).toBe(false);
  expect(bearerMatches("Bearer ", "")).toBe(false);
});

layer(Layer.empty)((it) => {
  it.effect("limitBytes and collectBytes fail as soon as a body passes the limit", () =>
    Effect.gen(function* () {
      const body = Stream.make(bytes(1, 2), bytes(3), bytes(4, 5));
      const tooLarge = (size: number) => new TooLarge({ size });

      expect(yield* collectBytes(body, 5, tooLarge)).toEqual(bytes(1, 2, 3, 4, 5));
      expect(yield* Effect.flip(collectBytes(body, 4, tooLarge))).toEqual(
        new TooLarge({ size: 5 }),
      );

      let pulled = 0;
      yield* Effect.exit(
        limitBytes(body.pipe(Stream.tap(() => Effect.sync(() => pulled++))), 2, tooLarge).pipe(
          Stream.runDrain,
        ),
      );
      expect(pulled).toBe(2);
    }),
  );

  it.effect("hashStream digests and writes each chunk, and stops at the limit", () =>
    Effect.gen(function* () {
      const written: Uint8Array[] = [];

      const result = yield* hashStream(
        Stream.make(new TextEncoder().encode("ab"), new TextEncoder().encode("c")),
        3,
        (size) => new TooLarge({ size }),
        (chunk) => Effect.sync(() => void written.push(chunk)),
      );

      expect(result).toEqual({
        hash: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        size: 3,
      });
      expect(written).toHaveLength(2);
      expect(
        yield* Effect.flip(
          hashStream(Stream.make(bytes(1, 2, 3, 4)), 3, (size) => new TooLarge({ size })),
        ),
      ).toEqual(new TooLarge({ size: 4 }));
    }),
  );
});
