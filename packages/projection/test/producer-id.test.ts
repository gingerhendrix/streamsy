// oxlint-disable-next-line effecttsgo/node-builtin-import -- a second SHA-256 checks the WebCrypto digest
import { createHash } from "node:crypto";
import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { MAX_PRODUCER_ID_LENGTH } from "@streamsy/core/internal";
import { producerId } from "../src/commit/producer.ts";

// oxlint-disable-next-line typescript/no-misused-spread -- storage counts code points
const length = (value: string) => [...value].length;

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

/** `files-wake/v1/{"folderId":"` and `"}` take 29 code points around the folder id. */
const wakeId = (folderId: string) => producerId("files-wake", 1, { folderId });

const unhashed = (folderId: string) => `files-wake/v1/{"folderId":"${folderId}"}`;

it("the limit is the storage producer id limit", () => {
  expect(MAX_PRODUCER_ID_LENGTH).toBe(255);
});

it.effect("an id of exactly 255 code points is unchanged", () =>
  Effect.gen(function* () {
    const folderId = "a".repeat(255 - 29);
    expect(length(unhashed(folderId))).toBe(255);
    expect(yield* wakeId(folderId)).toBe(unhashed(folderId));
  }),
);

it.effect(
  "an id of 256 code points becomes the id, the version and the SHA-256 of the full id",
  () =>
    Effect.gen(function* () {
      const folderId = "a".repeat(256 - 29);
      const full = unhashed(folderId);
      expect(length(full)).toBe(256);

      const hashed = yield* wakeId(folderId);
      expect(hashed).toBe(`files-wake/v1/#${sha256(full)}`);
      expect(length(hashed)).toBeLessThanOrEqual(255);
    }),
);

it.effect("non-ASCII params and ids are counted and cut by code point", () =>
  Effect.gen(function* () {
    // Each emoji is one code point and two UTF-16 code units.
    const fits = "😀".repeat(255 - 29);
    expect(yield* wakeId(fits)).toBe(unhashed(fits));

    const over = "😀".repeat(256 - 29);
    expect(yield* wakeId(over)).toBe(`files-wake/v1/#${sha256(unhashed(over))}`);

    const id = "é😀".repeat(150);
    const hashed = yield* producerId(id, 1, {});
    expect(length(hashed)).toBe(255);
    expect(hashed).toBe(`${"é😀".repeat(95)}#${sha256(`${id}/v1`)}`);
  }),
);

it.effect("a long projection id without params still fits", () =>
  Effect.gen(function* () {
    const id = "p".repeat(400);
    const hashed = yield* producerId(id, 3, {});
    expect(length(hashed)).toBe(255);
    expect(hashed).toBe(`${"p".repeat(190)}#${sha256(`${id}/v3`)}`);
  }),
);

it.effect("hashing is deterministic, and two long ids that share a prefix differ", () =>
  Effect.gen(function* () {
    const shared = "b".repeat(300);
    const first = yield* wakeId(`${shared}-one`);
    const second = yield* wakeId(`${shared}-two`);

    expect(yield* wakeId(`${shared}-one`)).toBe(first);
    expect(first.startsWith("files-wake/v1/#")).toBe(true);
    expect(second.startsWith("files-wake/v1/#")).toBe(true);
    expect(first).not.toBe(second);
  }),
);

it.effect("the version and the params are part of the digest", () =>
  Effect.gen(function* () {
    const folderId = "c".repeat(300);
    const v1 = yield* producerId("files-wake", 1, { folderId });
    const v2 = yield* producerId("files-wake", 2, { folderId });
    const other = yield* producerId("files-wake", 1, { folderId, user: "ann" });

    expect(new Set([v1, v2, other]).size).toBe(3);
  }),
);
