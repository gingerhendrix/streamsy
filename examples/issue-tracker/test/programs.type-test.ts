import type { Effect, Layer } from "effect";
import type { StreamsReader, StreamsWriter } from "@streamsy/core";
import type { State, Checkpoints } from "@streamsy/projection";
import type { HttpRouter } from "effect/unstable/http";
import type { SqlClient } from "effect/unstable/sql";
import type { app, outputRoutes, summaryDocument } from "../server/app.ts";
import type { checkCatalogWorkspace } from "../server/catalog.ts";
import type {
  applicationLayer,
  hostLayer,
  createInputs,
  ProjectionRunner,
} from "../server/host.ts";
import type { tracker, Board, LabelCounts, Transitions } from "../server/outputs.ts";
import type { issueRows } from "../server/projection.ts";
import type { prepareSchema } from "../server/schema.ts";
import type { transact } from "../server/state.ts";
import type { events, labelEvents, projects, users, labels } from "../server/streams.ts";

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;
type Inputs = events | labelEvents | projects | users | labels;
type Outputs = Board | LabelCounts | Transitions;
type RequestServices<R> = R extends HttpRouter.Request<"Requires", infer S> ? S : R;
type AppR = RequestServices<Layer.Services<ReturnType<typeof app>>>;
type OutputR = RequestServices<Layer.Services<typeof outputRoutes>>;
export type Checks = [
  Assert<Same<Effect.Services<ReturnType<typeof transact>>, events>>,
  Assert<Same<Effect.Services<ReturnType<typeof createInputs>>, Inputs>>,
  Assert<Same<Effect.Services<ReturnType<typeof checkCatalogWorkspace>>, never>>,
  Assert<Same<Effect.Services<typeof prepareSchema>, SqlClient.SqlClient>>,
  Assert<Same<Effect.Services<ReturnType<typeof summaryDocument.resolve>>, State>>,
  Assert<Same<Effect.Services<ReturnType<ProjectionRunner["Service"]["run"]>>, never>>,
  Assert<Same<Layer.Services<ReturnType<typeof applicationLayer>>, never>>,
  Assert<Same<Layer.Services<ReturnType<typeof hostLayer>>, never>>,
  Assert<
    Same<
      AppR,
      | HttpRouter.HttpRouter
      | Inputs
      | Outputs
      | State
      | SqlClient.SqlClient
      | Checkpoints
      | ProjectionRunner
    >
  >,
  Assert<Same<OutputR, HttpRouter.HttpRouter | Outputs | State>>,
  Assert<Same<Extract<AppR | OutputR, StreamsReader | StreamsWriter>, never>>,
  // The declarations' handlers still retain their SQL/State requirements; Host is supplied only by the host adapter.
  Assert<
    Same<
      Effect.Services<ReturnType<ReturnType<typeof issueRows.member>["process"]>>,
      SqlClient.SqlClient
    >
  >,
  Assert<Same<Effect.Services<ReturnType<ReturnType<typeof tracker.member>["process"]>>, State>>,
];
