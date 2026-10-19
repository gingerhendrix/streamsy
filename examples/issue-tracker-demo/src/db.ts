import { DateTime } from "effect";
import { createStreamDB, type StreamDB } from "@durable-streams/state/db";
import { Exit, Schema } from "effect";
import {
  Written,
  type CommentInput,
  type IssueInput,
  type ProjectInput,
} from "@streamsy/issue-tracker-demo-contract/api-schema";
import {
  issueTrackerState,
  type JsonValue,
} from "@streamsy/issue-tracker-demo-contract/state-schema";
import type {
  Comment,
  Issue,
  IssueStatus,
  Project,
} from "@streamsy/issue-tracker-demo-contract/types";
import { trackConnection, type Connection } from "./connection.ts";

export type OptimisticAction<T> = (variables: T) => { isPersisted: { promise: Promise<unknown> } };

export type CreateProjectAction = { project: Project; txid: string };

export type CreateIssueAction = { issue: Issue; txid: string };

/** A patch of the fields a user can edit, with the writer's display name. */
export type UpdateIssueAction = {
  issue: Issue;
  patch: { title?: string; status?: IssueStatus };
  updatedBy: string | undefined;
  txid: string;
};

export type CreateCommentAction = { comment: Comment; txid: string };

export type IssueDb = StreamDB<typeof issueTrackerState> & {
  actions: {
    createProject: OptimisticAction<CreateProjectAction>;
    createIssue: OptimisticAction<CreateIssueAction>;
    updateIssue: OptimisticAction<UpdateIssueAction>;
    createComment: OptimisticAction<CreateCommentAction>;
  };
  /** Whether the stream reader reaches the server. */
  connection: Connection;
};

/** A write the server refused, with the `error` message from its body. */
export class ApiError extends Schema.TaggedError<ApiError, { readonly brand: unique symbol }>()(
  "ApiError",
  {
    message: Schema.String,
  },
) {}

const ErrorBody = Schema.Struct({ error: Schema.String });

function streamUrl(workspaceId: string): string {
  return new URL(
    `/streams/workspace/${encodeURIComponent(workspaceId)}`,
    window.location.origin,
  ).toString();
}

function apiUrl(workspaceId: string, path: string): string {
  return `/api/w/${encodeURIComponent(workspaceId)}${path}`;
}

// The optimistic rows carry local times so they can sort at once. The bodies
// leave the times out: the server stamps them, and the stream row replaces the
// local one when the txid arrives.

export function createIssueDb(workspaceId: string): IssueDb {
  const connection = trackConnection();

  const streamDb = createStreamDB({
    streamOptions: {
      url: streamUrl(workspaceId),
      contentType: "application/json",
      warnOnHttp: false,
      fetch: connection.fetch,
    },
    state: issueTrackerState,
    actions: ({ db }) => ({
      createProject: {
        onMutate: ({ project }: CreateProjectAction) => {
          db.collections.projects.insert(project);
        },
        // oxlint-disable-next-line effecttsgo/async-function -- TanStack DB awaits this mutation Promise and rolls back rejected writes.
        mutationFn: async ({ project, txid }: CreateProjectAction) => {
          const body: ProjectInput = {
            id: project.id,
            name: project.name,
            description: project.description,
            txid,
          };

          const result = await postJson(apiUrl(workspaceId, "/projects"), body);
          await db.utils.awaitTxId(result.txid);
        },
      },
      createIssue: {
        onMutate: ({ issue }: CreateIssueAction) => {
          db.collections.issues.insert(issue);
        },
        // oxlint-disable-next-line effecttsgo/async-function -- TanStack DB awaits this mutation Promise and rolls back rejected writes.
        mutationFn: async ({ issue, txid }: CreateIssueAction) => {
          const body: IssueInput = {
            id: issue.id,
            projectId: issue.projectId,
            title: issue.title,
            status: issue.status,
            updatedBy: issue.updatedBy,
            txid,
          };

          const result = await postJson(apiUrl(workspaceId, "/issues"), body);
          await db.utils.awaitTxId(result.txid);
        },
      },
      updateIssue: {
        onMutate: ({ issue, patch, updatedBy }: UpdateIssueAction) => {
          db.collections.issues.update(issue.id, (draft) => {
            Object.assign(draft, patch);
            draft.updatedAt = DateTime.formatIso(DateTime.nowUnsafe());

            if (updatedBy === undefined) delete draft.updatedBy;
            else draft.updatedBy = updatedBy;
          });
        },
        // oxlint-disable-next-line effecttsgo/async-function -- TanStack DB awaits this mutation Promise and rolls back rejected writes.
        mutationFn: async ({ issue, patch, updatedBy, txid }: UpdateIssueAction) => {
          // JSON leaves out an undefined `updatedBy`.
          const body: IssueInput = { ...patch, updatedBy, txid };

          const result = await postJson(
            apiUrl(workspaceId, `/issues/${encodeURIComponent(issue.id)}`),
            body,
            { method: "PATCH" },
          );

          await db.utils.awaitTxId(result.txid);
        },
      },
      createComment: {
        onMutate: ({ comment }: CreateCommentAction) => {
          db.collections.comments.insert(comment);
        },
        // oxlint-disable-next-line effecttsgo/async-function -- TanStack DB awaits this mutation Promise and rolls back rejected writes.
        mutationFn: async ({ comment, txid }: CreateCommentAction) => {
          const body: CommentInput = {
            id: comment.id,
            issueId: comment.issueId,
            author: comment.author,
            body: comment.body,
            txid,
          };

          const result = await postJson(apiUrl(workspaceId, "/comments"), body);
          await db.utils.awaitTxId(result.txid);
        },
      },
    }),
  });

  return Object.assign(streamDb, { connection });
}

function postJson(url: string, body: JsonValue, init: RequestInit = {}): Promise<Written> {
  const headers = new Headers(init.headers);

  if (!headers.has("content-type")) headers.set("content-type", "application/json");

  // oxlint-disable-next-line effecttsgo/global-fetch -- This browser transport preserves native RequestInit and Response semantics.
  return fetch(url, {
    ...init,
    method: init.method ?? "POST",
    headers,
    body: JSON.stringify(body),
  }).then((response) => {
    if (!response.ok) return apiError(response).then((error) => Promise.reject(error));

    return response.json().then(Schema.decodeUnknownSync(Written));
  });
}

/** The `{ error }` body as an `ApiError`, or the status when the body is not one. */
function apiError(response: Response): Promise<ApiError> {
  return response.text().then((text) => {
    const decoded = Schema.decodeExit(Schema.fromJsonString(ErrorBody))(text);

    return new ApiError({
      message: Exit.isSuccess(decoded)
        ? decoded.value.error
        : `Request failed with status ${response.status}`,
    });
  });
}

export function awaitOptimisticAction<T>(action: OptimisticAction<T>, variables: T): Promise<void> {
  return action(variables).isPersisted.promise.then(() => {});
}
