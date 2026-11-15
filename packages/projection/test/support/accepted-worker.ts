/* oxlint-disable effecttsgo/async-function -- Worker fetch is a native promise boundary. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  Request as WorkerRequest,
  Response as WorkerResponse,
} from "@cloudflare/workers-types";
import { Layer, Schema } from "effect";
import { Http } from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionWakeup,
  Transaction,
} from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { Accepted, Source, group } from "./accepted-streams.ts";

const projection = Projection.fold({
  id: "accepted-worker",
  params: { id: Schema.String },
  inputs: { source: Source },
  accepted: Accepted,
  commit: Commit.transaction,
  state: { schema: Schema.Finite, initial: 0 },
  fold: (sum, entry) => sum + entry.item,
});

const layer = (state: DurableObjectState) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(DurableObjectStorage.layer(group, { client: { storage: state.storage } })),
  );

interface Env {
  readonly OBJECT: DurableObjectNamespace;
}

export class AcceptedObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof layer>>
>({
  layer,
  app: Http.routes(group, { access: "full" }),
  wakeups: [ProjectionWakeup.attach(projection)],
}) {}

export default {
  async fetch(request: WorkerRequest, env: Env): Promise<WorkerResponse> {
    return env.OBJECT.get(env.OBJECT.idFromName("one")).fetch(request);
  },
};
