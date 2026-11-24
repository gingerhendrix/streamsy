/* oxlint-disable effecttsgo/strict-effect-provide -- Each test allocates an isolated memory owner. */
import { expect, it } from "@effect/vitest";
import { CommitBoundary, Offset, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { enlist, ownerOf } from "@streamsy/core/internal";
import { Effect, Exit, Fiber, Stream } from "effect";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- The test commits private output relations.
import { makeLiveOutputs } from "../../src/live-output.ts";
import { memoryHost } from "../support/memory.ts";

const at = (position: number) =>
  Offset.make(`${"0".repeat(16)}_${String(position).padStart(16, "0")}`);

it.live("publishes nothing from a unit that fails after it enlists its output", () =>
  Effect.gen(function* () {
    const boundary = yield* CommitBoundary;
    const outputs = yield* makeLiveOutputs(["orders"], ["input"]);

    const unit = (key: string, offset: Offset) =>
      enlist(ownerOf(boundary), {
        onCommit: outputs.stage([{ view: "orders", kind: "upsert", key, value: { key } }], {
          input: offset,
        }),
        onRollback: () => {},
      });

    const initial = yield* outputs.snapshot;

    const received = yield* outputs.snapshots.pipe(
      Stream.runCollect,
      Effect.forkScoped({ startImmediately: true }),
    );

    expect(
      yield* boundary
        .withTransaction(unit("failed", at(1)).pipe(Effect.andThen(Effect.fail("rejected"))))
        .pipe(Effect.flip),
    ).toBe("rejected");
    expect(yield* outputs.snapshot).toBe(initial);
    expect(initial.offsets).toEqual({ input: ZERO_OFFSET });

    yield* boundary.withTransaction(unit("kept", at(2)));
    yield* outputs.end(Exit.void);

    const last = yield* outputs.snapshot;
    expect(last).toEqual({
      rows: { orders: [{ key: "kept" }] },
      offsets: { input: at(2) },
      caughtUp: false,
    });
    expect(yield* Fiber.join(received)).toEqual([initial, last]);
  }).pipe(Effect.provide(memoryHost(Streamsy.group()))),
);
