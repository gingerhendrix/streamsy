/**
 * The command edge, from the browser.
 *
 * Every mutation carries a `commandId` the browser generates once and reuses on
 * retry, so a retried request reconciles to its original acceptance instead of
 * creating a second event. Nothing on screen is built from these responses: the
 * board is rebuilt from the sink, so a second window sees the same thing.
 */
import { Schema } from "effect";
import type { IssueLabelRow, IssueStatus, IssueTransition } from "../../domain/issue.ts";
import { WorkspaceSummary } from "../../domain/issue.ts";
import type { InboxRow } from "../../domain/inbox.ts";
import {
  CommandResponse,
  InboxResponse,
  IssueLabelsResponse,
  LabelCommandResponse,
  NotificationsResponse,
  SeedResponse,
  TransitionFeedResponse,
} from "../../shared/api.ts";

export interface CommandAck {
  readonly commandId: string;
  readonly issueId: string;
  readonly reconciled: boolean;
  readonly ack: { readonly stream: string; readonly offset: string };
}

// oxlint-disable-next-line effecttsgo/extends-native-error -- This is the browser Promise boundary's HTTP rejection type, and React renders its native Error message.
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: string,
  ) {
    super(`${status}: ${body.slice(0, 200)}`);
    this.name = "ApiError";
  }
}

function randomId(prefix: string, length: number): string {
  // oxlint-disable-next-line effecttsgo/crypto-random-uuid -- Browser command/entity IDs require Web Crypto uniqueness and are created synchronously inside React event handlers.
  return `${prefix}-${crypto.randomUUID().replaceAll("-", "").slice(0, length)}`;
}

export function newCommandId(prefix: string): string {
  return randomId(prefix, 24);
}

export function newIssueId(): string {
  return randomId("issue", 12);
}

export function createIssue(
  workspaceId: string,
  input: {
    readonly commandId: string;
    readonly issueId: string;
    readonly projectId: string;
    readonly title: string;
    readonly status: IssueStatus;
  },
): Promise<CommandAck> {
  return post(CommandResponse, `/api/workspaces/${encodeURIComponent(workspaceId)}/issues`, input);
}

export function changeStatus(
  workspaceId: string,
  issueId: string,
  input: { readonly commandId: string; readonly status: IssueStatus },
): Promise<CommandAck> {
  return post(
    CommandResponse,
    `/api/workspaces/${encodeURIComponent(workspaceId)}/issues/${encodeURIComponent(issueId)}/status`,
    input,
  );
}

export function seedWorkspace(workspaceId: string): Promise<void> {
  return post(SeedResponse, `/api/workspaces/${encodeURIComponent(workspaceId)}/seed`, {}).then(
    () => undefined,
  );
}

export function attachLabel(
  workspaceId: string,
  issueId: string,
  input: { readonly commandId: string; readonly labelId: string },
): Promise<CommandAck> {
  return post(
    LabelCommandResponse,
    `/api/workspaces/${encodeURIComponent(workspaceId)}/issues/${encodeURIComponent(issueId)}/labels`,
    input,
  );
}

export function detachLabel(
  workspaceId: string,
  issueId: string,
  input: { readonly commandId: string; readonly labelId: string },
): Promise<CommandAck> {
  return post(
    LabelCommandResponse,
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

const LabelCatalogResponse = Schema.Struct({
  rows: Schema.Array(Schema.Struct({ labelId: Schema.String, name: Schema.String })),
});

export function fetchWorkspaceReadModels(workspaceId: string): Promise<WorkspaceReadModels> {
  const id = encodeURIComponent(workspaceId);
  return Promise.all([
    getJson(IssueLabelsResponse, `/api/workspaces/${id}/issue-labels`),
    getJson(LabelCatalogResponse, `/api/workspaces/${id}/catalog/labels`),
    getJson(WorkspaceSummary, `/document/workspaces/${id}/summary`),
    getJson(TransitionFeedResponse, `/feed/workspaces/${id}/issue-transitions`),
    getJson(NotificationsResponse, `/api/workspaces/${id}/notifications`),
  ]).then(([issueLabels, labelCatalog, summary, activity, notifications]) => ({
    issueLabels: issueLabels.rows,
    labelCatalog: labelCatalog.rows,
    summary,
    activity: activity.events,
    notifications,
  }));
}

/**
 * One user's cross-workspace inbox.
 *
 * The inbox is served by the *user* partition, which owns no durable stream
 * storage, so it has no checked sink and no resumable session — it is polled.
 * See `integration-2-decisions.md`; the UI says so rather than implying live
 * convergence it does not have.
 */
export function fetchInbox(userId: string): Promise<readonly InboxRow[]> {
  return getJson(InboxResponse, `/api/users/${encodeURIComponent(userId)}/inbox`).then(
    (body) => body.rows,
  );
}

function browserFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  return (
    // oxlint-disable-next-line effecttsgo/global-fetch -- This adapter is the browser's native transport boundary; its public API stays Promise-based for React event handlers.
    globalThis.fetch(input, init)
  );
}

function decodeJson<S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  text: string,
): Promise<S["Type"]> {
  return Schema.decodePromise(Schema.fromJsonString(schema))(text);
}

function getJson<S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  path: string,
): Promise<S["Type"]> {
  return browserFetch(path, { headers: { accept: "application/json" } })
    .then((response) =>
      response.text().then((text) => {
        if (!response.ok) throw new ApiError(response.status, text);
        return text;
      }),
    )
    .then((text) => decodeJson(schema, text));
}

/** Every request body this module sends: a command, or an empty seed request. */
type CommandBody = Record<string, string>;

function post<S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  path: string,
  body: CommandBody,
): Promise<S["Type"]> {
  return browserFetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
    .then((response) =>
      response.text().then((text) => {
        if (!response.ok) throw new ApiError(response.status, text);
        return text;
      }),
    )
    .then((text) => decodeJson(schema, text));
}
