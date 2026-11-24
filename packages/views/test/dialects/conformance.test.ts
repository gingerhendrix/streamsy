import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { engineConformance } from "../sql/conformance-suite.ts";
import { createBackend, serverFixtures } from "../support/backends.ts";

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} engine conformance`, () => {
    it.live(
      "matches the independent oracle for 25 seeds × 40 units",
      () => Effect.flatMap(createBackend(fixture), (backend) => engineConformance(backend, 25)),
      120_000,
    );

    it.live(
      "matches the oracle for 25 seeds × 40 units with long common key prefixes",
      () =>
        Effect.flatMap(createBackend(fixture), (backend) =>
          engineConformance(backend, 25, "x".repeat(1040)),
        ),
      120_000,
    );
  });
}
