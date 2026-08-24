/**
 * The command edge, from the browser.
 *
 * Every mutation carries a `commandId` the browser generates once and reuses on
 * retry, so a retried request reconciles to its original acceptance instead of
 * creating a second event. Nothing on screen is built from these responses: the
 * board is rebuilt from the sink, so a second window sees the same thing.
 */
import type { IssueStatus } from "../../domain/issue.ts";

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
