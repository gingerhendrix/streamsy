import { createStreamDB, type StreamDB } from "@durable-streams/state/db";
import { issueTrackerState } from "../shared/state-schema.ts";
import type { Comment, Issue, IssueStatus, Project } from "../shared/types.ts";

export type OptimisticAction<T> = (variables: T) => { isPersisted: { promise: Promise<unknown> } };

type ApiMutationResult = { awaitOffset: string; txid: string };

/** The mutation endpoints answer with the appended offset and its txid. */
function isApiMutationResult(value: unknown): value is ApiMutationResult {
  return (
    typeof value === "object" &&
    value !== null &&
    "awaitOffset" in value &&
    typeof value.awaitOffset === "string" &&
    "txid" in value &&
    typeof value.txid === "string"
  );
}

export type CreateProjectAction = { project: Project; txid: string };
export type CreateIssueAction = { issue: Issue; txid: string };
export type UpdateIssueStatusAction = {
  issue: Issue;
  status: IssueStatus;
  updatedAt: string;
  txid: string;
};
export type CreateCommentAction = { comment: Comment; txid: string };

export type IssueDb = StreamDB<typeof issueTrackerState> & {
  actions: {
    createProject: OptimisticAction<CreateProjectAction>;
    createIssue: OptimisticAction<CreateIssueAction>;
    updateIssueStatus: OptimisticAction<UpdateIssueStatusAction>;
    createComment: OptimisticAction<CreateCommentAction>;
  };
};

function streamUrl(workspaceId: string): string {
  return new URL(
    `/streams/workspace/${encodeURIComponent(workspaceId)}`,
    window.location.origin,
  ).toString();
}

function apiUrl(workspaceId: string, path: string): string {
  return `/api/w/${encodeURIComponent(workspaceId)}${path}`;
}

export function createIssueDb(workspaceId: string): IssueDb {
  return createStreamDB({
    streamOptions: {
      url: streamUrl(workspaceId),
      contentType: "application/json",
      warnOnHttp: false,
    },
    state: issueTrackerState,
    actions: ({ db }) => ({
      createProject: {
        onMutate: ({ project }: CreateProjectAction) => {
          db.collections.projects.insert(project);
        },
        mutationFn: async ({ project, txid }: CreateProjectAction) => {
          const result = await postJson(apiUrl(workspaceId, "/projects"), {
            ...project,
            txid,
          });
          await db.utils.awaitTxId(result.txid);
        },
      },
      createIssue: {
        onMutate: ({ issue }: CreateIssueAction) => {
          db.collections.issues.insert(issue);
        },
        mutationFn: async ({ issue, txid }: CreateIssueAction) => {
          const result = await postJson(apiUrl(workspaceId, "/issues"), {
            ...issue,
            txid,
          });
          await db.utils.awaitTxId(result.txid);
        },
      },
      updateIssueStatus: {
        onMutate: ({ issue, status, updatedAt }: UpdateIssueStatusAction) => {
          db.collections.issues.update(issue.id, (draft) => {
            draft.status = status;
            draft.updatedAt = updatedAt;
          });
        },
        mutationFn: async ({ issue, status, updatedAt, txid }: UpdateIssueStatusAction) => {
          const result = await postJson(
            apiUrl(workspaceId, `/issues/${encodeURIComponent(issue.id)}`),
            { status, updatedAt, txid },
            { method: "PATCH" },
          );
          await db.utils.awaitTxId(result.txid);
        },
      },
      createComment: {
        onMutate: ({ comment }: CreateCommentAction) => {
          db.collections.comments.insert(comment);
        },
        mutationFn: async ({ comment, txid }: CreateCommentAction) => {
          const result = await postJson(apiUrl(workspaceId, "/comments"), {
            ...comment,
            txid,
          });
          await db.utils.awaitTxId(result.txid);
        },
      },
    }),
  });
}

async function postJson(
  url: string,
  body: unknown,
  init: RequestInit = {},
): Promise<ApiMutationResult> {
  const headers = new Headers(init.headers);
  if (!headers.has("content-type")) headers.set("content-type", "application/json");
  const response = await fetch(url, {
    ...init,
    method: init.method ?? "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(await response.text());
  const payload: unknown = await response.json();
  if (!isApiMutationResult(payload)) {
    throw new Error(`Unexpected mutation response from ${url}`);
  }
  return payload;
}

export async function awaitOptimisticAction<T>(
  action: OptimisticAction<T>,
  variables: T,
): Promise<void> {
  const transaction = action(variables);
  await transaction.isPersisted.promise;
}
