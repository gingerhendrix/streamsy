import { Streamsy } from "@streamsy/core";
import type { Effect, Layer } from "effect";
import type { Storage } from "@streamsy/core";
import { Projection, type State, type Checkpoints } from "@streamsy/projection";
import type { HttpRouter } from "effect/unstable/http";
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
type RequestServices<R> = R extends HttpRouter.Request<"Requires", infer S> ? S : R;
type AppR = RequestServices<Layer.Services<ReturnType<typeof app>>>;
type OutputR = RequestServices<Layer.Services<typeof outputRoutes>>;
const sqlRun = (address: ReturnType<typeof issueRows.at>) => Projection.run(address);
const outputRun = (address: ReturnType<typeof tracker.at>) => Projection.run(address);
const sqlWatch = (address: ReturnType<typeof issueRows.at>) => Projection.watch(address);
const outputWatch = (address: ReturnType<typeof tracker.at>) => Projection.watch(address);
export type Checks = [
  Assert<
    Same<Effect.Services<ReturnType<typeof sqlRun>>, Checkpoints | Inputs | SqlClient.SqlClient>
  >,
  Assert<
    Same<Effect.Services<ReturnType<typeof outputRun>>, Checkpoints | Inputs | Outputs | State>
  >,
  Assert<
    Same<
      Effect.Services<ReturnType<typeof sqlWatch>>,
      | Checkpoints
      | Inputs
      | Streamsy.ChangeFeed<Inputs>
      | SqlClient.SqlClient
      | import("effect").Scope.Scope
    >
  >,
  Assert<
    Same<
      Effect.Services<ReturnType<typeof outputWatch>>,
      | Checkpoints
      | Inputs
      | Streamsy.ChangeFeed<Inputs>
      | Outputs
      | State
      | import("effect").Scope.Scope
    >
  >,
  Assert<
    Same<
      Extract<
        | Layer.Success<ReturnType<typeof applicationLayer>>
        | Layer.Success<ReturnType<typeof hostLayer>>,
        Storage
      >,
      never
    >
  >,
  Assert<Same<Effect.Services<ReturnType<typeof transact>>, Events>>,
  Assert<Same<Effect.Services<ReturnType<typeof createInputs>>, Inputs>>,
  Assert<Same<Effect.Services<ReturnType<typeof checkCatalogWorkspace>>, never>>,
  Assert<Same<Effect.Services<typeof prepareSchema>, SqlClient.SqlClient>>,
  Assert<Same<Effect.Services<ReturnType<typeof workspaceSummary.resolve>>, State>>,
  Assert<Same<Layer.Services<ReturnType<typeof applicationLayer>>, never>>,
  Assert<Same<Layer.Services<ReturnType<typeof hostLayer>>, never>>,
  Assert<
    Same<AppR, HttpRouter.HttpRouter | Inputs | Outputs | State | SqlClient.SqlClient | Checkpoints>
  >,
  Assert<Same<OutputR, HttpRouter.HttpRouter | Outputs | State>>,
  // Handlers retain their SQL/State requirements. Runners add declared streams and checkpoints.
  Assert<
    Same<
      Effect.Services<ReturnType<ReturnType<typeof issueRows.at>["process"]>>,
      SqlClient.SqlClient
    >
  >,
  Assert<Same<Effect.Services<ReturnType<ReturnType<typeof tracker.at>["process"]>>, State>>,
];
