import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect } from "effect";
import { SyncState, stateLayer } from "../../sync/state.ts";

layer(stateLayer(":memory:"))((it) => {
  it.effect(
    "settlement rebases queued observations and rejection retains an immutable content source",
    () =>
      Effect.gen(function* () {
        const state = yield* SyncState;

        const known = {
          folderId: "docs",
          path: "a.md",
          version: 1,
          localVersion: 1,
          hash: "a".repeat(64),
          localHash: "b".repeat(64),
        };

        for (const txid of ["first", "second"])
          yield* state.enqueue(
            {
              folderId: "docs",
              path: "a.md",
              txid,
              base: 1,
              hash: "b".repeat(64),
              size: 9,
              source: "/cache/immutable",
              uploaded: 0,
            },
            known,
          );
        const first = (yield* state.pending())[0];

        if (first === undefined) return yield* Effect.die("Missing pending observation");
        yield* state.settle(first, 2);
        const second = (yield* state.pending())[0];
        expect(second?.base).toBe(2);

        if (second === undefined) return yield* Effect.die("Missing rebased observation");
        yield* state.reject(second.seq, 413, "too large");
        expect(yield* state.pending()).toEqual([]);
        expect((yield* state.rejected())[0]).toMatchObject({
          source: "/cache/immutable",
          size: 9,
          status: 413,
        });
        expect((yield* state.head("docs", "a.md"))?.localVersion).toBe(2);

        return yield* Effect.void;
      }),
  );
});
