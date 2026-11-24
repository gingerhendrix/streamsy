/* oxlint-disable effecttsgo/strict-effect-provide -- Each test allocates an isolated memory owner. */
import { describe, it } from "@effect/vitest";
import { CommitBoundary, Streamsy } from "@streamsy/core";
import { Array as Arr, Effect, Predicate, Record } from "effect";
import type { Backend } from "../../src/backend.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { engineConformance } from "../sql/conformance-suite.ts";
import { registerViewConformance } from "../sql/view-conformance-suite.ts";
import { memory, memoryHost } from "../support/memory.ts";

function deepFreeze<A>(value: A): A {
  if (Predicate.isObject(value)) {
    for (const child of Record.values(value)) deepFreeze(child);
    Object.freeze(value);
  } else if (Arr.isArray(value)) {
    for (const child of value) deepFreeze(child);
    Object.freeze(value);
  }

  return value;
}

function freezing(backend: Backend): Backend {
  return {
    ...backend,
    store: (namespace) => {
      const store = backend.store(namespace);

      return {
        ...store,
        put: (relation, entries) => store.put(relation, Arr.map(entries, deepFreeze)),
        indexPut: (arrangement, entries) =>
          store.indexPut(arrangement, Arr.map(entries, deepFreeze)),
        accumulatorPut: (node, key, value) => store.accumulatorPut(node, key, deepFreeze(value)),
      };
    },
  };
}

describe("Memory Views conformance", () => {
  for (const freeze of [false, true]) {
    it.live(
      `matches the independent oracle for 200 seeds × 40 units${freeze ? " with frozen values" : ""}`,
      () =>
        Effect.gen(function* () {
          const backend = yield* currentBackend;
          const boundary = yield* CommitBoundary;
          const checked = freeze ? freezing(backend) : backend;

          yield* engineConformance(
            {
              ...checked,
              atomic: (run) => boundary.withTransaction(checked.atomic(run)),
            },
            200,
          );
        }).pipe(Effect.provide(memoryHost(Streamsy.group()))),
      120_000,
    );
  }

  registerViewConformance(memory);
});
