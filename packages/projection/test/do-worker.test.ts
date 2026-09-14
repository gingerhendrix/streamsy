import type { DurableObjectState } from "@cloudflare/workers-types";
import { Layer, ManagedRuntime } from "effect";
import { Protocol } from "@streamsy/core";
import * as DurableObjectStorage from "@streamsy/storage/durable-object";
import * as Sqlite from "../src/sqlite.ts";
import { composition } from "./scenarios.ts";
interface Env {
  readonly PROJECTION: {
    readonly idFromName: (name: string) => DurableObjectId;
    readonly get: (id: DurableObjectId) => { readonly fetch: (input: string) => Promise<Response> };
  };
}
export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.PROJECTION.get(env.PROJECTION.idFromName("projection-proof")).fetch(request.url);
  },
};
export class ProjectionObject {
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
