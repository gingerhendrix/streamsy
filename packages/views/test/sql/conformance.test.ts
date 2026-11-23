import { it } from "@effect/vitest";
import { Effect } from "effect";
import { createBackend, sqlite } from "../support/backends.ts";
import { engineConformance } from "./conformance-suite.ts";

it.live(
  "matches the independent oracle: 200 seeds × 40 random multi-change units, with exact no-op suppression",
  () => Effect.flatMap(createBackend(sqlite), (backend) => engineConformance(backend, 200)),
  120_000,
);
