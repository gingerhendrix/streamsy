/* oxlint-disable effecttsgo/strict-effect-provide -- These tests deliberately provide the wrong backend service. */
import { expect, it } from "@effect/vitest";
import { CheckpointStore, NonRetryable, OwnerTransaction, Projection } from "@streamsy/projection";
import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/sql";
import { View, ViewBackend, ViewFault } from "../../src/index.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { backendFor, client, hostFor, sqlite } from "../support/backends.ts";
import { sqlDump } from "../support/layout-dump.ts";
import {
  editLayout,
  layoutBinding,
  layoutCircuit,
  layoutGroup,
  layoutHead,
  layoutNamespace,
  layoutOutput,
  layoutParams,
  startLayout,
} from "./support/layout.ts";

const host = hostFor(sqlite, layoutGroup);

const dump = sqlDump(layoutNamespace, false);

const secondClient = Effect.gen(function* () {
  const sql = Context.get(yield* Layer.build(client(sqlite)), SqlClient.SqlClient);
  yield* View.migrate(sql);

  const layer = ViewBackend.layerSql.pipe(
    Layer.fresh,
    Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
  );

  return { sql, layer };
});

for (const backend of ["second SQL client", "memory on a SQL owner"] as const) {
  it.live(`rejects ${backend} during a unit, forget and retire`, () =>
    Effect.gen(function* () {
      const second = backend === "second SQL client" ? yield* secondClient : undefined;

      if (second !== undefined) {
        const hostClient = yield* SqlClient.SqlClient;
        expect(second.sql).not.toBe(hostClient);
        expect(second.sql.transactionService.key).not.toBe(hostClient.transactionService.key);
      }

      const bad = Context.get(
        second === undefined
          ? yield* Layer.build(ViewBackend.layerMemory)
          : yield* Layer.build(second.layer),
        ViewBackend,
      );

      const message =
        second === undefined
          ? "Views memory state requires the active memory owner transaction on this fiber"
          : "The Views SQL client is outside the storage owner's transaction";

      const failOnWrongBackend = <A, E, R>(run: Effect.Effect<A, E, R>) =>
        run.pipe(Effect.provideService(ViewBackend, bad), Effect.flip);

      const foreignDump =
        second === undefined
          ? Effect.void
          : dump.pipe(Effect.provideService(SqlClient.SqlClient, second.sql));

      yield* startLayout;
      yield* editLayout;
      const store = yield* CheckpointStore;
      const before = yield* dump;
      const foreignBefore = yield* foreignDump;

      const failed = yield* failOnWrongBackend(Projection.run(layoutBinding));
      expect(failed).toBeInstanceOf(ViewFault);
      expect(failed).toMatchObject({ message, [NonRetryable]: true });
      expect((yield* store.load(layoutBinding)).token).toBe("0");
      expect(yield* layoutHead.pipe(Effect.flip)).toMatchObject({ _tag: "StreamNotFound" });
      expect(yield* dump).toEqual(before);
      expect(yield* foreignDump).toEqual(foreignBefore);

      // Populate both databases so cleanup has state to remove.
      yield* Projection.run(layoutBinding);
      const localState = yield* dump;
      const saved = yield* store.load(layoutBinding);
      const output = yield* layoutOutput;

      if (second !== undefined) {
        const other = yield* backendFor(second.sql);
        yield* other.circuit(layoutNamespace).save("other database");
        yield* other.store(layoutNamespace).put("rows", [{ key: "keep", row: { value: 1 } }]);
      }

      const otherState = yield* foreignDump;

      const cleanup = [
        Projection.forget(layoutBinding),
        Projection.retire(layoutCircuit, layoutParams, { generation: layoutBinding.generation }),
      ] as const;

      for (const run of cleanup) {
        const failure = yield* failOnWrongBackend(Effect.asVoid(run));
        expect(failure).toMatchObject({
          phase: "checkpoint",
          reason: "unsupported-composition",
          message,
          retryable: false,
          cause: { _tag: "ViewFault", message, [NonRetryable]: true },
        });
        expect(yield* store.load(layoutBinding)).toEqual(saved);
        expect(yield* layoutOutput).toEqual(output);
        expect(yield* dump).toEqual(localState);
        expect(yield* foreignDump).toEqual(otherState);
      }
    }).pipe(Effect.provide(host)),
  );
}

it.live("layerMemoryHost supplies a shared owner for operator state and checkpoints", () =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;
    const owner = yield* OwnerTransaction;
    const backend = yield* currentBackend;
    const state = backend.circuit(layoutNamespace);
    const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
    expect(sql).toEqual(Option.none());

    expect((yield* store.load(layoutBinding)).token).toBe("0");
    expect(
      yield* owner
        .withTransaction(state.save("plan").pipe(Effect.andThen(Effect.fail("abort"))))
        .pipe(Effect.flip),
    ).toBe("abort");
    expect(yield* owner.withTransaction(state.load)).toBeUndefined();
    yield* owner.withTransaction(state.save("plan"));
    expect(yield* owner.withTransaction(state.load)).toBe("plan");
  }).pipe(Effect.provide(ViewBackend.layerMemoryHost)),
);
