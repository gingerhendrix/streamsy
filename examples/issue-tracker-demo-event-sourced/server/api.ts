import { Effect, Layer } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { HttpApiBuilder, HttpApiMiddleware, type HttpApiError } from "effect/http-api";
import { RequestErrors, IssueTrackerApi } from "@streamsy/issue-tracker-demo-contract/api";
import { BadRequest } from "@streamsy/issue-tracker-demo-contract/api-schema";
import { CommentsLive } from "./routes/comments.ts";
import { IssuesLive } from "./routes/issues.ts";
import { ProjectsLive } from "./routes/projects.ts";
import { WorkspacesLive } from "./routes/workspaces.ts";

/** A request that fails its schema answers 400 with a readable `error`. */
const RequestErrorsLive = HttpApiMiddleware.layerSchemaErrorTransform(RequestErrors, (error) =>
  Effect.fail(new BadRequest({ error: requestError(error) })),
);

function requestError({ kind, cause }: HttpApiError.HttpApiSchemaError): string {
  if (kind === "Params") return "Invalid workspace id";

  return `Invalid request ${kind.toLowerCase()}: ${cause.message}`;
}

/** Registers every IssueTrackerApi endpoint on the shared HttpRouter. */
export const ApiLive = HttpApiBuilder.layer(IssueTrackerApi).pipe(
  Layer.provide([WorkspacesLive, ProjectsLive, IssuesLive, CommentsLive]),
  Layer.provide(RequestErrorsLive),
);

/** Other `/api` paths answer a JSON 404 instead of falling through to the page. */
export const ApiNotFoundLive = HttpRouter.add(
  "*",
  "/api/*",
  HttpServerResponse.jsonUnsafe({ error: "Not found" }, { status: 404 }),
);
