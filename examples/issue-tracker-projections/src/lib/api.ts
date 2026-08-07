/** Typed browser client for the demo API. Every failure surfaces a message. */
import type {
  ApiError,
  BoardResponse,
  CreateIssueRequest,
  CreateProjectRequest,
  HealthResponse,
  IssueCommandRequest,
  MutationResponse,
  ProjectsResponse,
} from "../../shared/api.ts";
import type { IssueDetail, Project } from "../../shared/model.ts";

export class ApiFailure extends Error {
  constructor(
    override readonly message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiFailure";
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      cache: "no-store",
      ...(init?.body === undefined
        ? {}
        : { headers: { "content-type": "application/json", ...init.headers } }),
    });
  } catch (error) {
    throw new ApiFailure(error instanceof Error ? error.message : "Network error", 0);
  }
  const text = await response.text();
  if (!response.ok) {
    let detail = text.slice(0, 200);
    try {
      const parsed = JSON.parse(text) as ApiError;
      detail = parsed.detail ? `${parsed.error}: ${parsed.detail}` : parsed.error;
    } catch {
      // Non-JSON error bodies stay as raw text.
    }
    throw new ApiFailure(detail, response.status);
  }
  return (text.length === 0 ? undefined : JSON.parse(text)) as T;
}

const workspacePath = (workspaceId: string): string =>
  `/api/workspaces/${encodeURIComponent(workspaceId)}`;

export const api = {
  health: (): Promise<HealthResponse> => request<HealthResponse>("/health"),

  seed: (workspaceId: string): Promise<unknown> =>
    request(`${workspacePath(workspaceId)}/seed`, { method: "POST" }),

  listProjects: (workspaceId: string): Promise<readonly Project[]> =>
    request<ProjectsResponse>(`${workspacePath(workspaceId)}/projects`).then(
      (response) => response.projects,
    ),

  createProject: (workspaceId: string, body: CreateProjectRequest): Promise<Project> =>
    request<Project>(`${workspacePath(workspaceId)}/projects`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  board: (workspaceId: string, projectId: string): Promise<BoardResponse> =>
    request<BoardResponse>(
      `${workspacePath(workspaceId)}/projects/${encodeURIComponent(projectId)}/board`,
    ),

  repair: (workspaceId: string, projectId: string): Promise<unknown> =>
    request(`${workspacePath(workspaceId)}/projects/${encodeURIComponent(projectId)}/repair`, {
      method: "POST",
    }),

  createIssue: (workspaceId: string, body: CreateIssueRequest): Promise<MutationResponse> =>
    request<MutationResponse>(`${workspacePath(workspaceId)}/issues`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  issueCommand: (
    workspaceId: string,
    issueId: string,
    body: IssueCommandRequest,
  ): Promise<MutationResponse> =>
    request<MutationResponse>(
      `${workspacePath(workspaceId)}/issues/${encodeURIComponent(issueId)}/commands`,
      { method: "POST", body: JSON.stringify(body) },
    ),

  issueDetail: (workspaceId: string, issueId: string): Promise<IssueDetail> =>
    request<IssueDetail>(`${workspacePath(workspaceId)}/issues/${encodeURIComponent(issueId)}`),
};

/** Identifiers must satisfy the server's stream-segment rule. */
export function newId(prefix: string): string {
  const random = globalThis.crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  return `${prefix}${random}`;
}
