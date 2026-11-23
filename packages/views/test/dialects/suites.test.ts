import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { registerApi } from "../circuit/api-suite.ts";
import { registerLifecycle } from "../circuit/lifecycle-suite.ts";
import { routedGroup, routedProof } from "../circuit/support/routed-proof.ts";
import { registerViewConformance } from "../sql/view-conformance-suite.ts";
import { createBackend, hostFor, serverFixtures } from "../support/backends.ts";
import { storeContract } from "../support/store-contract.ts";

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} Views`, () => {
    storeContract("Store", () => createBackend(fixture));
    registerApi(fixture);
    registerLifecycle(fixture);
    registerViewConformance(fixture);

    it.live("routes the full-recompute oracle by workspace and date", () =>
      routedProof(false).pipe(
        Effect.scoped,
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test provisions its own dialect host.
        Effect.provide(hostFor(fixture, routedGroup)),
        Effect.tap((result) =>
          Effect.sync(() => expect(result).toEqual({ routes: 3, moved: true, faults: 2 })),
        ),
      ),
    );
  });
}
