import { Schema } from "effect";
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiMiddleware,
  HttpApiSchema,
} from "effect/unstable/httpapi";
import {
  BadRequest,
  CommentInput,
  CommentWritten,
  Conflict,
  InternalServerError,
  IssueInput,
  IssueWritten,
  NotFound,
  ProjectInput,
  ProjectWritten,
  WorkspaceCreated,
  WorkspaceId,
} from "./api-schema.ts";

/** Turns a request that fails its schema into a 400 with an `error` message. */
export class RequestErrors extends HttpApiMiddleware.Service<RequestErrors>()(
  "IssueTrackerDemo/RequestErrors",
  { error: BadRequest },
) {}

const inWorkspace = { ws: WorkspaceId };
const writeErrors = [BadRequest, NotFound, Conflict, InternalServerError];

export class WorkspacesApi extends HttpApiGroup.make("workspaces").add(
  HttpApiEndpoint.post("create", "/workspaces", {
    success: WorkspaceCreated.pipe(HttpApiSchema.status(201)),
    error: [BadRequest, NotFound, Conflict, InternalServerError],
  }),
) {}

export class ProjectsApi extends HttpApiGroup.make("projects").add(
  HttpApiEndpoint.post("create", "/w/:ws/projects", {
    params: inWorkspace,
    payload: ProjectInput,
    success: ProjectWritten.pipe(HttpApiSchema.status(201)),
    error: writeErrors,
  }),
) {}

export class IssuesApi extends HttpApiGroup.make("issues").add(
  HttpApiEndpoint.post("create", "/w/:ws/issues", {
    params: inWorkspace,
    payload: IssueInput,
    success: IssueWritten.pipe(HttpApiSchema.status(201)),
    error: writeErrors,
  }),
  HttpApiEndpoint.patch("update", "/w/:ws/issues/:id", {
    params: { ...inWorkspace, id: Schema.String },
    payload: IssueInput,
    success: IssueWritten,
    error: writeErrors,
  }),
) {}

export class CommentsApi extends HttpApiGroup.make("comments").add(
  HttpApiEndpoint.post("create", "/w/:ws/comments", {
    params: inWorkspace,
    payload: CommentInput,
    success: CommentWritten.pipe(HttpApiSchema.status(201)),
    error: writeErrors,
  }),
) {}

/** The app's own JSON API. Streamsy serves `/streams` beside it. */
export class IssueTrackerApi extends HttpApi.make("IssueTrackerApi")
  .add(WorkspacesApi, ProjectsApi, IssuesApi, CommentsApi)
  .middleware(RequestErrors)
  .prefix("/api") {}
