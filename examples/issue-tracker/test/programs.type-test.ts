import { Streamsy } from "@streamsy/core";
import type { Effect, Layer } from "effect";
import type { Storage } from "@streamsy/core";
import { Projection, type Transaction, type CheckpointStore } from "@streamsy/projection";
import type { HttpRouter, HttpServerRequest } from "effect/unstable/http";
import type { SqlClient } from "effect/unstable/sql";
import type { app, outputRoutes, workspaceSummary } from "../server/app.ts";
import type { checkCatalogWorkspace } from "../server/catalog.ts";
import type { applicationLayer, hostLayer, createInputs } from "../server/host.ts";
import type { tracker, Board, LabelCounts, Transitions } from "../server/outputs.ts";
import type { issueRows } from "../server/projection.ts";
import type { prepareSchema } from "../server/schema.ts";
import type { transact } from "../server/state.ts";
import type { Events, LabelEvents, Projects, Users, Labels } from "../server/streams.ts";

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;
type Inputs = Events | LabelEvents | Projects | Users | Labels;
type Outputs = Board | LabelCounts | Transitions;
type RequestServices<R> =
  R extends HttpRouter.Request<"Requires", infer S>
    ? S
    : R extends HttpRouter.Request<"Error", infer _E>
      ? never
      : R;
type AppR = RequestServices<Layer.Services<ReturnType<typeof app>>>;
type OutputR = RequestServices<Layer.Services<typeof outputRoutes>>;
const sqlRun = (address: ReturnType<typeof issueRows.at>) => Projection.run(address);
const outputRun = (address: ReturnType<typeof tracker.at>) => Projection.run(address);
const sqlWatch = (address: ReturnType<typeof issueRows.at>) => Projection.watch(address);
const outputWatch = (address: ReturnType<typeof tracker.at>) => Projection.watch(address);
export type Checks = [
  Assert<Same<Effect.Services<ReturnType<typeof sqlRun>>, Transaction | CheckpointStore | Inputs>>,
  Assert<Same<Effect.Services<ReturnType<typeof outputRun>>, CheckpointStore | Inputs | Outputs>>,
  Assert<
    Same<
      Effect.Services<ReturnType<typeof sqlWatch>>,
      | Transaction
      | CheckpointStore
      | Inputs
      | Streamsy.ChangeFeed<Inputs>
      | import("effect").Scope.Scope
    >
  >,
  Assert<
    Same<
      Effect.Services<ReturnType<typeof outputWatch>>,
      | CheckpointStore
      | Inputs
      | Streamsy.ChangeFeed<Inputs>
      | Outputs
      | import("effect").Scope.Scope
    >
  >,
  // The application layer adds startup work and no services to the host.
  Assert<
    Same<
      Layer.Success<ReturnType<typeof applicationLayer>>,
      Layer.Success<ReturnType<typeof hostLayer>>
    >
  >,
  Assert<Same<Extract<Layer.Success<ReturnType<typeof hostLayer>>, Storage>, never>>,
  // `transact` runs `issueRows` and reads its SQL tables before it appends.
  Assert<
    Same<
      Effect.Services<ReturnType<typeof transact>>,
      Transaction | CheckpointStore | Inputs | SqlClient.SqlClient
    >
  >,
  Assert<Same<Effect.Services<ReturnType<typeof createInputs>>, Inputs>>,
  Assert<Same<Effect.Services<ReturnType<typeof checkCatalogWorkspace>>, never>>,
  Assert<Same<Effect.Services<typeof prepareSchema>, SqlClient.SqlClient>>,
  Assert<
    Same<
      Effect.Services<typeof workspaceSummary>,
      CheckpointStore | HttpRouter.RouteContext | HttpServerRequest.HttpServerRequest
    >
  >,
  Assert<Same<Layer.Services<ReturnType<typeof applicationLayer>>, never>>,
  Assert<Same<Layer.Services<ReturnType<typeof hostLayer>>, never>>,
  Assert<
    Same<
      AppR,
      HttpRouter.HttpRouter | Inputs | Outputs | Transaction | SqlClient.SqlClient | CheckpointStore
    >
  >,
  Assert<Same<OutputR, HttpRouter.HttpRouter | Outputs | CheckpointStore>>,
  // Handlers retain SQL; the transaction Commit supplies it to runners.
  Assert<
    Same<
      Effect.Services<ReturnType<ReturnType<typeof issueRows.at>["process"]>>,
      SqlClient.SqlClient
    >
  >,
  Assert<Same<Effect.Services<ReturnType<ReturnType<typeof tracker.at>["process"]>>, never>>,
];
