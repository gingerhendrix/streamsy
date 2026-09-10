/* oxlint-disable effecttsgo/async-function -- Local workerd's fetch owns the runtime edge. */
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Layer, ManagedRuntime } from "effect";
import { Protocol } from "@streamsy/core";
import * as DurableObjectStorage from "@streamsy/storage/durable-object";
import * as Sqlite from "../src/sqlite.ts";
import { composition } from "./scenarios.ts";
interface Env {
  readonly DERIVE: {
    readonly idFromName: (name: string) => DurableObjectId;
    readonly get: (id: DurableObjectId) => { readonly fetch: (input: string) => Promise<Response> };
  };
}
export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.DERIVE.get(env.DERIVE.idFromName("derive-proof")).fetch(request.url);
  },
};
export class DeriveObject {
  readonly #ctx: DurableObjectState;
  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx;
  }
  async fetch(): Promise<Response> {
    const host = Layer.merge(Protocol.layer(), Sqlite.layer).pipe(
      Layer.provideMerge(DurableObjectStorage.layer({ client: { storage: this.#ctx.storage } })),
    );
    const runtime = ManagedRuntime.make(host);
    try {
      return Response.json(await runtime.runPromise(composition));
    } finally {
      await runtime.dispose();
    }
  }
}
