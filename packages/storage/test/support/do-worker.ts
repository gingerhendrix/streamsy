/* oxlint-disable effecttsgo/async-function, eslint/no-underscore-dangle -- Fetch is the real workerd edge; outcomes use public `_tag`. */
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Cause, Context, Effect, Exit, Layer, ManagedRuntime, Option } from "effect";
import { CommitBoundary, Storage, StreamId, ZERO_OFFSET, type Mutation } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { makeBoundaryTestProbe, sharedSqlClientLayer } from "../../src/boundary.ts";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { STORAGE_MIGRATIONS, STORAGE_VERSION_TABLE_DDL } from "../../src/schema.ts";
import { layerWithTestProbe } from "../../src/storage.ts";
import {
  finishOwnerCleanupProof,
  prepareOwnerCleanupProof,
  runBoundaryScenarios,
  runExternalRepair,
} from "./storage-boundary-scenarios.ts";

import {
  projectionMarksContract,
  projectionMarksTransactionContract,
} from "../../../core/src/testing/projection-marks-contract.ts";

interface ObjectId {
  readonly toString: () => string;
}

interface Env {
  readonly STORAGE: {
    readonly idFromName: (name: string) => ObjectId;
    readonly get: (id: ObjectId) => { readonly fetch: (input: string) => Promise<Response> };
  };
}

const createStream = (id: StreamId): Mutation => ({
  operations: [
    {
      _tag: "Create",
      record: {
        id,
        config: { contentType: "text/plain", createdAt: 0 },
        lifecycle: { closed: false, softDeleted: false },
        currentOffset: ZERO_OFFSET,
      },
      initialMessages: [],
    },
  ],
});

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.STORAGE.get(
      env.STORAGE.idFromName(
        new URL(request.url).pathname === "/upgrade" ? "migration" : "batch-b",
      ),
    ).fetch(request.url);
  },
};

export class StorageObject {
  readonly #ctx: DurableObjectState;

  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx;
  }

  #layer(repairIntervalMs: number, probe: ReturnType<typeof makeBoundaryTestProbe>) {
    const client = Layer.effectContext(
      sharedSqlClientLayer(SqliteClient.make({ storage: this.#ctx.storage })),
    );

    return layerWithTestProbe({ repairIntervalMs }, probe).pipe(Layer.provideMerge(client));
  }

  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === "/upgrade") {
      const sql = this.#ctx.storage.sql;

      if (
        sql.exec("SELECT name FROM sqlite_master WHERE name = 'streamsy_schema'").toArray()
          .length === 0
      ) {
        sql.exec(STORAGE_VERSION_TABLE_DDL.sqlite);

        for (const migration of STORAGE_MIGRATIONS.slice(0, 2))
          for (const statement of migration.sqlite) sql.exec(statement);
        sql.exec("INSERT INTO streamsy_schema VALUES (1, 1), (2, 2)");
        sql.exec(
          "INSERT INTO streamsy_projection_pending VALUES ('kept', 'a', 7, 3, 1234567890123)",
        );
      }

      const runtime = ManagedRuntime.make(
        DurableObjectStorage.layerProtocol({ client: { storage: this.#ctx.storage } }),
      );

      const row = await runtime.runPromise(
        Effect.gen(function* () {
          const marks = yield* ProjectionMarks;

          return (yield* marks.read("kept", ["a"])).get("a");
        }),
      );

      await runtime.dispose();

      return Response.json({
        row,
        version: sql.exec("SELECT MAX(version) AS version FROM streamsy_schema").one().version,
      });
    }

    const boundaryProbe = makeBoundaryTestProbe();
    boundaryProbe.repairsPaused = true;
    const boundaryRuntime = ManagedRuntime.make(this.#layer(60_000, boundaryProbe));

    const boundary = await boundaryRuntime.runPromise(
      runBoundaryScenarios(boundaryProbe).pipe(Effect.scoped),
    );

    const pendingCleanup = await boundaryRuntime.runPromise(
      prepareOwnerCleanupProof(boundaryProbe),
    );

    await boundaryRuntime.dispose();

    const ownerCleanup = await Effect.runPromise(
      finishOwnerCleanupProof(boundaryProbe, pendingCleanup),
    );

    const repairProbe = makeBoundaryTestProbe();
    const repairRuntime = ManagedRuntime.make(this.#layer(100, repairProbe));

    const repair = await repairRuntime.runPromise(
      runExternalRepair(
        Effect.sync(() => {
          this.#ctx.storage.sql.exec(
            "INSERT INTO streamsy_streams VALUES ('external','text/plain',NULL,NULL,0,'0000000000000000_0000000000000000',NULL,0,NULL,NULL,NULL,NULL,0,NULL)",
          );
        }),
        repairProbe,
      ).pipe(Effect.scoped),
    );

    await repairRuntime.dispose();

    const entryRuntime = ManagedRuntime.make(
      DurableObjectStorage.layerProtocol({ client: { storage: this.#ctx.storage } }),
    );

    const entry = await entryRuntime.runPromise(
      Effect.gen(function* () {
        const storage = yield* Storage;
        const marks = yield* ProjectionMarks;

        const summary = (now: number) =>
          marks.summary("summary", now).pipe(
            Effect.map((row) => ({
              ...row,
              nextDueAt: Option.getOrNull(row.nextDueAt),
            })),
          );

        const empty = yield* summary(3_000);
        yield* Effect.orDie(marks.mark("summary", ["fresh"]));
        yield* marks.fail("summary", "past", {
          attempts: 1,
          dueAt: Option.some(2_000),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });
        yield* marks.fail("summary", "now", {
          attempts: 2,
          dueAt: Option.some(3_000),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });
        yield* marks.fail("summary", "future", {
          attempts: 3,
          dueAt: Option.some(4_000),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });
        yield* marks.fail("other", "future", {
          attempts: 1,
          dueAt: Option.some(5_000),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });
        const mixed = yield* summary(3_000);
        yield* marks.remove("summary", "fresh");
        const allDue = yield* summary(4_000);

        yield* projectionMarksContract(marks);
        yield* projectionMarksTransactionContract(marks, yield* CommitBoundary);

        const outcome = yield* storage.mutate(createStream(StreamId.make("exported-entry")));

        return {
          outcome: outcome._tag,
          present: Option.isSome(yield* storage.record(StreamId.make("exported-entry"))),
          capabilities: storage.capabilities,
          markSummaries: { empty, mixed, allDue },
          marksContractPassed: true,
        };
      }),
    );

    const sharedStorage = await entryRuntime.runPromise(
      Effect.gen({ self: this }, function* () {
        const second = yield* Layer.build(
          DurableObjectStorage.layerProtocol({ client: { storage: this.#ctx.storage } }),
        );

        const storage = yield* Storage;
        const firstBoundary = yield* CommitBoundary;
        const secondStorage = Context.get(second, Storage);
        const secondBoundary = Context.get(second, CommitBoundary);
        const nested = StreamId.make("shared-storage-nested");
        const lone = StreamId.make("shared-storage-lone");
        let visibleInside = false;

        const exit = yield* firstBoundary
          .withTransaction(
            Effect.gen(function* () {
              yield* secondBoundary.withTransaction(secondStorage.mutate(createStream(nested)));
              yield* secondStorage.mutate(createStream(lone));
              visibleInside =
                Option.isSome(yield* storage.record(nested)) &&
                Option.isSome(yield* storage.record(lone));

              return yield* Effect.fail("rollback shared storage");
            }),
          )
          .pipe(Effect.exit);

        return {
          distinctBoundaries: firstBoundary !== secondBoundary,
          visibleInside,
          failedAsExpected:
            Exit.isFailure(exit) &&
            Option.contains(Cause.findErrorOption(exit.cause), "rollback shared storage"),
          rolledBack:
            Option.isNone(yield* storage.record(nested)) &&
            Option.isNone(yield* storage.record(lone)),
        };
      }).pipe(Effect.scoped),
    );

    await entryRuntime.dispose();

    return Response.json({ boundary, ownerCleanup, repair, entry, sharedStorage });
  }
}
