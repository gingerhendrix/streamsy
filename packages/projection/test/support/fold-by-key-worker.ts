import type { DurableObjectState } from "@cloudflare/workers-types";
import { Array, Effect, Layer, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { CheckpointStore, Projection, StateTable, Transaction } from "@streamsy/projection";
import { Facts, Workspace, Table, sums } from "./fold-by-key.ts";

interface Env {
  readonly FOLD: {
    readonly idFromName: (name: string) => DurableObjectId;
    readonly get: (id: DurableObjectId) => { readonly fetch: (input: string) => Promise<Response> };
  };
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.FOLD.get(env.FOLD.idFromName("fold-proof")).fetch(request.url);
  },
};

export class FoldObject {
  readonly #ctx: DurableObjectState;
  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx;
  }

  fetch(): Promise<Response> {
    const local = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(
        DurableObjectStorage.layerProtocol({
          client: { storage: this.#ctx.storage },
          readLimit: 200,
        }),
      ),
    );

    const host = Protocol.bind(local, Streamsy.group(Facts, Workspace)).pipe(
      Layer.provideMerge(local),
    );

    return Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const services = yield* Layer.build(host);

          return yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* StateTable.migrate(sql);
            const values = { workspace: "workerd", source: "source" };
            const facts = yield* Facts;
            yield* facts.create(values);

            const records = Array.makeBy(113, (index) => ({
              id: String(index).padStart(3, "0"),
              n: 1,
            }));

            yield* facts.append(values, records);
            const binding = sums().at(values);
            yield* Projection.run(binding);
            const lagging = (yield* Table.at(values).list("sum")).rows.length;
            // A second unit reads previous values in three chunks: 50, 50 and 13 keys.
            yield* facts.append(values, records);
            yield* Projection.run(binding);
            yield* Projection.run(Table.at(values));
            const rows = (yield* Table.at(values).list("sum")).rows;
            const output = yield* (yield* Workspace).read(values).pipe(Stream.runCollect);

            return Response.json({
              lagging,
              rows: Array.map(rows, (row) => row.value),
              outputCount: Array.reduce(output, 0, (n, batch) => n + batch.items.length),
            });
          }).pipe(Effect.provideContext(services));
        }),
      ),
    );
  }
}
