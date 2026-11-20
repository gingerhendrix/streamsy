/* oxlint-disable effecttsgo/async-function -- The Worker router delegates to native fetch. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  ExportedHandler,
} from "@cloudflare/workers-types";
import { Effect, Layer, Option } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { SqlClient } from "effect/sql";
import { Http, Streamsy } from "@streamsy/core";
import { CheckpointStore, Commit, Projection, StateTable, Transaction } from "@streamsy/projection";
import { Placement, router, rule, StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import {
  Accepted,
  Facts,
  Numbers,
  Rows,
  failedHandler,
  numbers,
  params,
  sums,
  table,
} from "./generation-outputs.ts";

interface Env {
  readonly OBJECT: DurableObjectNamespace;
  readonly GENERATION: number;
}

const group = Streamsy.group(Facts, Numbers, Rows, Accepted);

const owner = ({ workspace }: { readonly workspace: string }) => `workspace:${workspace}`;

const streams = router<Env>({
  namespace: (env) => env.OBJECT,
  prefix: "/streams",
  placement: Placement.byRoute([
    rule({ stream: Facts, owner }),
    rule({ stream: Numbers, owner }),
    rule({ stream: Rows, owner }),
    rule({ stream: Accepted, owner }),
  ]),
});

const layer = (state: DurableObjectState) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(DurableObjectStorage.layer(group, { client: { storage: state.storage } })),
  );

const run = HttpRouter.add(
  "GET",
  "/proof/:generation/:mode",
  Effect.gen(function* () {
    const path = yield* HttpRouter.params;
    const generation = Number(path.generation);
    const bound = numbers(Commit.transaction, generation, path.mode === "fail").at(params);
    const result = yield* Projection.run(bound).pipe(Effect.result);
    const store = yield* CheckpointStore;
    const record = yield* store.load(bound);

    if (result._tag === "Failure")
      return HttpServerResponse.jsonUnsafe({
        handlerFailed: result.failure === failedHandler,
        checkpoint: Option.isSome(record.record),
      });

    yield* StateTable.migrate(yield* SqlClient.SqlClient);
    yield* Projection.run(sums(generation).at(params));
    const materialized = table.at({ ...params, rev: generation });
    yield* Projection.run(materialized);

    return HttpServerResponse.jsonUnsafe({
      checkpoint: Option.isSome(record.record),
      count: (yield* materialized.get("sum", "a")).value?.count,
      replayed: result.success.items,
    });
  }).pipe(Effect.orDie),
);

export class GenerationObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof layer>>
>({
  layer,
  app: Layer.mergeAll(Http.routes(group, { prefix: "/streams", access: "full" }), run),
}) {}

const worker: ExportedHandler<Env> = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/proof/run" || url.pathname === "/proof/fail") {
      const mode = url.pathname === "/proof/fail" ? "fail" : "run";
      url.pathname = `/proof/${env.GENERATION}/${mode}`;

      return env.OBJECT.get(env.OBJECT.idFromName(owner(params))).fetch(url.toString());
    }

    return streams.fetch!(request, env, ctx);
  },
};

export default worker;
