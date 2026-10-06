import type { Effect, Layer } from "effect";
import type { Transaction, CheckpointStore } from "@streamsy/projection";
import type { HttpRouter } from "effect/http";
import type { SqlClient } from "effect/sql";
import type { app, outputRoutes } from "../server/app.ts";
import type { tracker, Board, LabelCounts, Transitions } from "../server/outputs.ts";
import type { issueRows } from "../server/projection.ts";
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

export type Checks = [
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
