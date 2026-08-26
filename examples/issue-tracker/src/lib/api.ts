/**
 * The command edge, from the browser.
 *
 * Every mutation carries a `commandId` the browser generates once and reuses on
 * retry, so a retried request reconciles to its original acceptance instead of
 * creating a second event. Nothing on screen is built from these responses: the
 * board is rebuilt from the sink, so a second window sees the same thing.
 */
import type { IssueLabelRow, IssueStatus, IssueTransition } from "../../domain/issue.ts";
import type { InboxRow } from "../../domain/inbox.ts";
import type { WorkspaceSummary } from "../../domain/issue.ts";

export interface CommandAck {
  readonly commandId: string;
  readonly issueId: string;
  readonly reconciled: boolean;
  readonly ack: { readonly stream: string; readonly offset: string };
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: string,
  ) {
    super(`${status}: ${body.slice(0, 200)}`);
    this.name = "ApiError";
  }
}

export function newCommandId(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().replaceAll("-", "").slice(0, 24)}`;
}

export async function createIssue(
  workspaceId: string,
  input: {
    readonly commandId: string;
    readonly issueId: string;
    readonly projectId: string;
    readonly title: string;
    readonly status: IssueStatus;
  },
): Promise<CommandAck> {
  return post(`/api/workspaces/${encodeURIComponent(workspaceId)}/issues`, input);
}

export async function changeStatus(
  workspaceId: string,
  issueId: string,
  input: { readonly commandId: string; readonly status: IssueStatus },
): Promise<CommandAck> {
  return post(
    `/api/workspaces/${encodeURIComponent(workspaceId)}/issues/${encodeURIComponent(issueId)}/status`,
    input,
  );
}

export async function seedWorkspace(workspaceId: string): Promise<void> {
  await post(`/api/workspaces/${encodeURIComponent(workspaceId)}/seed`, {});
}

export async function attachLabel(
  workspaceId: string,
  issueId: string,
  input: { readonly commandId: string; readonly labelId: string },
): Promise<CommandAck> {
  return post(
    `/api/workspaces/${encodeURIComponent(workspaceId)}/issues/${encodeURIComponent(issueId)}/labels`,
    input,
  );
}

export async function detachLabel(
  workspaceId: string,
  issueId: string,
  input: { readonly commandId: string; readonly labelId: string },
): Promise<CommandAck> {
  return post(
    `/api/workspaces/${encodeURIComponent(workspaceId)}/issues/${encodeURIComponent(issueId)}/labels/detach`,
    input,
  );
}

/**
 * The read models the browser polls.
 *
 * The board and the label counts arrive over their checked State sinks, so they
 * converge without asking. Everything below is a *read model over maintained
 * state* rather than a published contract, and the browser refreshes it after
 * every command and on a slow interval. That difference is deliberate and it is
 * visible on screen: each polled panel says when it last refreshed.
 */
export interface WorkspaceReadModels {
  readonly issueLabels: readonly IssueLabelRow[];
  readonly labelCatalog: readonly { readonly labelId: string; readonly name: string }[];
  readonly summary: WorkspaceSummary;
  readonly activity: readonly IssueTransition[];
  readonly notifications: {
    readonly pending: number;
    readonly delivered: number;
    readonly dead: number;
  };
}

export async function fetchWorkspaceReadModels(workspaceId: string): Promise<WorkspaceReadModels> {
  const id = encodeURIComponent(workspaceId);
  const [issueLabels, labelCatalog, summary, activity, notifications] = await Promise.all([
    getJson<{ rows: readonly IssueLabelRow[] }>(`/api/workspaces/${id}/issue-labels`),
    getJson<{ rows: readonly { labelId: string; name: string }[] }>(
      `/api/workspaces/${id}/catalog/labels`,
    ),
    getJson<WorkspaceSummary>(`/document/workspaces/${id}/summary`),
    getJson<{ events: readonly IssueTransition[] }>(`/feed/workspaces/${id}/issue-transitions`),
    getJson<{ pending: number; delivered: number; dead: number }>(
      `/api/workspaces/${id}/notifications`,
    ),
  ]);
  return {
    issueLabels: issueLabels.rows,
    labelCatalog: labelCatalog.rows,
    summary,
    activity: activity.events,
    notifications,
  };
}

/**
 * One user's cross-workspace inbox.
 *
 * The inbox is served by the *user* partition, which owns no durable stream
 * storage, so it has no checked sink and no resumable session — it is polled.
 * See `integration-2-decisions.md`; the UI says so rather than implying live
 * convergence it does not have.
 */
export async function fetchInbox(userId: string): Promise<readonly InboxRow[]> {
  const body = await getJson<{ rows: readonly InboxRow[] }>(
    `/api/users/${encodeURIComponent(userId)}/inbox`,
  );
  return body.rows;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path, { headers: { accept: "application/json" } });
  const text = await response.text();
  if (!response.ok) throw new ApiError(response.status, text);
  // SAFETY: every path here is one of this application's own routes, and each
  // one serves the wire contract named at the call site. The panels read only
  // the fields those contracts declare, and a shape change would surface as a
  // missing value on screen rather than as a bad command.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  return JSON.parse(text) as T;
}

/** Every request body this module sends: a command, or an empty seed request. */
type CommandBody = Record<string, string>;

async function post(path: string, body: CommandBody): Promise<CommandAck> {
  const response = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) throw new ApiError(response.status, text);
  // SAFETY: a 2xx from this server is the `CommandResponse` contract in
  // `shared/api.ts`; `CommandAck` names the subset this module reads, and a
  // non-2xx has already been thrown above.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  return JSON.parse(text) as CommandAck;
}
