import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect } from "effect";
import { SyncState, stateLayer } from "../../sync/state.ts";

layer(stateLayer(":memory:"))((it) => {
  it.effect(
    "rejecting a rebased save settles the observed version and retains the rejected bytes",
    () =>
      Effect.gen(function* () {
        const state = yield* SyncState;
        const version = 1;
        const hash = "a".repeat(64);

        const known = {
          folderId: "docs",
          path: "a.md",
          version,
          localVersion: version,
          hash,
          localHash: hash,
        };

        yield* state.enqueue(
          {
            folderId: "docs",
            path: "a.md",
            base: version,
            opId: "save1",
            bytes: new TextEncoder().encode("save1"),
            hash,
          },
          known,
        );
        yield* state.enqueue(
          {
            folderId: "docs",
            path: "a.md",
            base: version,
            opId: "save2",
            bytes: new TextEncoder().encode("save2"),
            hash,
          },
          known,
        );
        const first = (yield* state.pending())[0];

        if (first === undefined) return yield* Effect.die("Missing first save");
        const receipt = 2;
        yield* state.dropAndRebase(first.seq, known, version, receipt);
        const second = (yield* state.pending())[0];

        if (second === undefined) return yield* Effect.die("Missing second save");
        yield* state.reject(second.seq, 413, "rejected save");
        expect((yield* state.head("docs", "a.md"))?.version).toBe(receipt);
        expect((yield* state.head("docs", "a.md"))?.localVersion).toBe(receipt);
        expect(yield* state.pending()).toEqual([]);
        expect(new TextDecoder().decode((yield* state.rejected())[0]?.bytes ?? undefined)).toBe(
          "save2",
        );

        return yield* Effect.void;
      }),
  );
});
