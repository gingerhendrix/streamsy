/* oxlint-disable effecttsgo/async-function, eslint/no-underscore-dangle -- Fetch is the real workerd edge; outcomes use public `_tag`. */
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Effect, Layer, ManagedRuntime, Option } from "effect";
import { Storage, StreamId, ZERO_OFFSET } from "@streamsy/core";
import { makeBoundaryTestProbe, sharedSqlClientLayer } from "../src/boundary.ts";
import { layer as durableObjectLayer } from "../src/durable-object.ts";
import { layerWithTestProbe } from "../src/storage.ts";
import {
  finishOwnerCleanupProof,
  prepareOwnerCleanupProof,
  runBoundaryScenarios,
  runExternalRepair,
} from "./storage-boundary-scenarios.ts";

interface ObjectId {
  readonly toString: () => string;
}

interface Env {
  readonly STORAGE: {
    readonly idFromName: (name: string) => ObjectId;
    readonly get: (id: ObjectId) => { readonly fetch: (input: string) => Promise<Response> };
  };
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.STORAGE.get(env.STORAGE.idFromName("batch-b")).fetch(request.url);
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

  async fetch(): Promise<Response> {
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
      durableObjectLayer({ client: { storage: this.#ctx.storage } }),
    );
    const entry = await entryRuntime.runPromise(
      Effect.gen(function* () {
        const storage = yield* Storage;
        const outcome = yield* storage.mutate({
          operations: [
            {
              _tag: "Create",
              record: {
                id: StreamId.make("exported-entry"),
                config: { contentType: "text/plain", createdAt: 0 },
                lifecycle: { closed: false, softDeleted: false },
                currentOffset: ZERO_OFFSET,
              },
              initialMessages: [],
            },
          ],
        });
        return {
          outcome: outcome._tag,
          present: Option.isSome(yield* storage.record(StreamId.make("exported-entry"))),
          capabilities: storage.capabilities,
        };
      }),
    );
    await entryRuntime.dispose();
    return Response.json({ boundary, ownerCleanup, repair, entry });
  }
}
