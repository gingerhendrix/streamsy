import { expect, layer } from "@streamsy/effect-test";
import { Effect } from "effect";
import { feedEpoch, rotateFeedEpoch } from "../server/feed-epoch.ts";
import { sqliteHost } from "../server/storage.ts";

layer(sqliteHost(":memory:"))((it) => {
  it.effect("a Bun index epoch is stable until a rebuild rotates it", () =>
    Effect.gen(function* () {
      const original = yield* feedEpoch;
      expect(yield* feedEpoch).toBe(original);
      const rebuilt = yield* rotateFeedEpoch;
      expect(rebuilt).not.toBe(original);
      expect(yield* feedEpoch).toBe(rebuilt);
    }),
  );
});
