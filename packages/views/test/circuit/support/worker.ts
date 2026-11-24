import type { DurableObjectState, DurableObjectId } from "@cloudflare/workers-types";
import { Effect, Layer } from "effect";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { ViewBackend } from "../../../src/index.ts";
import { Shop, Results } from "./shop.ts";
import { proof } from "./proof.ts";
import { routedGroup, routedProof } from "./routed-proof.ts";

interface Env {
  readonly SHOP: {
    readonly idFromName: (name: string) => DurableObjectId;
    readonly get: (id: DurableObjectId) => { readonly fetch: (input: string) => Promise<Response> };
  };
}

export default {
  fetch: (request: Request, env: Env) =>
    env.SHOP.get(env.SHOP.idFromName(new URL(request.url).pathname)).fetch(request.url),
};

export class ShopObject {
  readonly #ctx: DurableObjectState;
  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx;
  }
  async fetch(request: Request): Promise<Response> {
    const cursors: { readonly rowsRead: number; readonly rowsWritten: number }[] = [];
    const sql = this.#ctx.storage.sql;

    // Instrument the native SQL boundary; keep cursors until the client has
    // consumed them, then read workerd's actual billable-row counters.
    // oxlint-disable anti-slop/no-reflect-get -- A transparent proxy preserves native host getters and receiver-bound methods.
    const measuredSql = new Proxy(sql, {
      get(target, key) {
        if (key === "exec")
          return (...args: Parameters<typeof sql.exec>) => {
            const cursor = target.exec(...args);
            cursors.push(cursor);

            return cursor;
          };

        const value = Reflect.get(target, key, target);

        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Native host proxy boundary binds methods to their required receiver.
        return typeof value === "function" ? value.bind(target) : value;
      },
    });

    const storage = new Proxy(this.#ctx.storage, {
      get(target, key) {
        if (key === "sql") return measuredSql;
        const value = Reflect.get(target, key, target);

        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Native host proxy boundary binds methods to their required receiver.
        return typeof value === "function" ? value.bind(target) : value;
      },
    });

    // oxlint-enable anti-slop/no-reflect-get
    const local = Layer.mergeAll(
      CheckpointStore.layer,
      Transaction.layer,
      ViewBackend.layerSql,
    ).pipe(
      Layer.provideMerge(
        DurableObjectStorage.layerProtocol({ client: { storage }, readLimit: 200 }),
      ),
    );

    if (new URL(request.url).pathname === "/routed")
      return Response.json(
        await Effect.runPromise(
          routedProof(true).pipe(
            Effect.scoped,
            Effect.provide(Protocol.bind(local, routedGroup).pipe(Layer.provideMerge(local))),
          ),
        ),
      );

    const host = Protocol.bind(local, Streamsy.group(Shop, Results)).pipe(
      Layer.provideMerge(local),
    );

    const result = await Effect.runPromise(
      proof({
        reset: () => {
          cursors.length = 0;
        },
        read: () => {
          let rowsRead = 0,
            rowsWritten = 0;

          for (const cursor of cursors) {
            rowsRead += cursor.rowsRead;
            rowsWritten += cursor.rowsWritten;
          }

          return { rowsRead, rowsWritten };
        },
      }).pipe(Effect.provide(host)),
    );

    return Response.json(result);
  }
}
