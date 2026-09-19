import { expect, it } from "bun:test";
import { Effect, Schema } from "effect";
import { StreamId } from "../../src/schema/index.ts";
import { expectFailureTag } from "../../src/testing/expect-failure-tag.ts";
import { OffsetMismatch, NotSupported } from "../../src/protocol/errors.ts";

it("protocol errors round-trip through Schema and recover by tag", async () => {
  const id = StreamId.make("s");
  const error = new OffsetMismatch({ id, expected: "stale", actual: "tail" });
  const encoded = Schema.encodeSync(OffsetMismatch)(error);
  const decoded = Schema.decodeUnknownSync(OffsetMismatch)(encoded);
  expect(decoded).toBeInstanceOf(OffsetMismatch);
  await Effect.runPromise(
    Effect.gen(function* () {
      expect(yield* expectFailureTag(Effect.fail(decoded), "OffsetMismatch")).toMatchObject({
        id,
        expected: "stale",
        actual: "tail",
      });
      expect(
        yield* Effect.fail(decoded).pipe(
          Effect.catchTag("OffsetMismatch", (e) => Effect.succeed(e.actual)),
        ),
      ).toBe("tail");
      expect(Schema.encodeSync(NotSupported)(new NotSupported({ id, feature: "fork" }))).toEqual({
        _tag: "NotSupported",
        id,
        feature: "fork",
      });
    }),
  );
});
