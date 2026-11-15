import { CheckpointStore, Transaction } from "@streamsy/projection";
import * as FamilyFixture from "./families.ts";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Effect, Layer } from "effect";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { entryOffsets } from "./entry-offsets.ts";
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
  fetch(request: Request): Promise<Response> {
    const offsets = new URL(request.url).pathname === "/offsets";

    const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(
        DurableObjectStorage.layerProtocol({
          client: { storage: this.#ctx.storage },
          readLimit: offsets ? 3 : 1,
        }),
      ),
    );

    return Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const services = yield* Layer.build(FamilyFixture.withFamilies(host));

          return Response.json(
            offsets
              ? yield* entryOffsets.pipe(Effect.provideContext(services))
              : yield* composition.pipe(Effect.provideContext(services)),
          );
        }),
      ),
    );
  }
}
