/* oxlint-disable effecttsgo/strict-effect-provide -- Each test allocates an isolated memory owner. */
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { registerApi } from "../circuit/api-suite.ts";
import { registerLifecycle } from "../circuit/lifecycle-suite.ts";
import { routedGroup, routedProof } from "../circuit/support/routed-proof.ts";
import { memory, memoryHost } from "../support/memory.ts";

describe("Memory Views circuits", () => {
  registerApi(memory);
  registerLifecycle(memory);

  it.live("routes the full-recompute oracle by workspace and date", () =>
    Effect.gen(function* () {
      expect(yield* routedProof(false)).toEqual({ routes: 3, moved: true, faults: 2 });
    }).pipe(Effect.provide(memoryHost(routedGroup))),
  );
});
