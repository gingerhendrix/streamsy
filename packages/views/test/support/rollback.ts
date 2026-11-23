/* oxlint-disable effecttsgo/strict-effect-provide -- This probe replaces only the failing checkpoint write. */
import { expect, it } from "@effect/vitest";
import { CommitBoundary } from "@streamsy/core";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect } from "effect";
import { ViewBackend } from "../../src/index.ts";
import {
  editLayout,
  layoutBinding,
  layoutGroup,
  layoutNamespace,
  layoutOutput,
  runLayoutUnits,
} from "../circuit/support/layout.ts";
import { logicalDump } from "./layout-dump.ts";
import { circuitHost, type Fixture } from "./memory.ts";

const dump = logicalDump(layoutNamespace);

export function registerRollback(fixture: Fixture) {
  const host = circuitHost(fixture, layoutGroup);

  it.live(`${fixture.name} rolls back Views writes when settling a unit fails`, () =>
    Effect.gen(function* () {
      yield* runLayoutUnits;
      yield* editLayout;
      const checkpoint = yield* CheckpointStore;
      const before = yield* dump;
      const output = yield* layoutOutput;
      const token = yield* checkpoint.load(layoutBinding);

      const rejected = new ProjectionFault({
        phase: "checkpoint",
        reason: "token-conflict",
        message: "reject test checkpoint after the operator writes",
      });

      const duringSettle = dump.pipe(
        Effect.provideService(CommitBoundary, yield* CommitBoundary),
        Effect.provideService(ViewBackend, yield* ViewBackend),
        Effect.orDie,
        Effect.tap((during) => Effect.sync(() => expect(during).not.toEqual(before))),
      );

      const failure = yield* Projection.run(layoutBinding).pipe(
        Effect.provideService(CheckpointStore, {
          ...checkpoint,
          compareAndSet: () => duringSettle.pipe(Effect.andThen(rejected)),
        }),
        Effect.flip,
      );

      expect(failure).toBe(rejected);
      expect(yield* dump).toEqual(before);
      expect(yield* checkpoint.load(layoutBinding)).toEqual(token);
      expect(yield* layoutOutput).toEqual(output);
      expect((yield* Projection.run(layoutBinding)).units).toBe(1);
      expect(yield* dump).not.toEqual(before);
    }).pipe(Effect.provide(host)),
  );

  it.live(`${fixture.name} rolls back a completed unit when its enclosing owner fails`, () =>
    Effect.gen(function* () {
      yield* runLayoutUnits;
      yield* editLayout;
      const boundary = yield* CommitBoundary;
      const before = yield* dump;
      const output = yield* layoutOutput;
      const checkpoint = yield* CheckpointStore;
      const token = yield* checkpoint.load(layoutBinding);
      const abort = "later owner failure";

      const outer = boundary.withTransaction(
        Effect.gen(function* () {
          expect((yield* Projection.run(layoutBinding)).units).toBe(1);
          expect(yield* dump).not.toEqual(before);
          expect(yield* checkpoint.load(layoutBinding)).not.toEqual(token);
          expect(yield* layoutOutput).not.toEqual(output);

          return yield* Effect.fail(abort);
        }),
      );

      expect(yield* outer.pipe(Effect.flip)).toBe(abort);
      expect(yield* dump).toEqual(before);
      expect(yield* checkpoint.load(layoutBinding)).toEqual(token);
      expect(yield* layoutOutput).toEqual(output);
    }).pipe(Effect.provide(host)),
  );
}
