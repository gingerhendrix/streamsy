/** Typed browser client for the demo API. Every failure surfaces a message. */
import type {
  ApiError,
  BoardResponse,
  CoverageResponse,
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
    const headers = new Headers(init?.headers);
    if (init?.body !== undefined && !headers.has("content-type"))
      headers.set("content-type", "application/json");
    response = await fetch(path, {
      ...init,
      cache: "no-store",
      headers,
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

/**
 * Ask the server to skip the immediate projection passes. Durability and the
 * acknowledgement are unchanged; the command simply has to converge through
 * repair, which is how the `Pending` path is exercised for real.
 */
export interface CommandOptions {
  readonly deferProjections?: boolean;
}

const deferQuery = (options: CommandOptions): string =>
  options.deferProjections === true ? "?projections=deferred" : "";

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

  createIssue: (
    workspaceId: string,
    body: CreateIssueRequest,
    options: CommandOptions = {},
  ): Promise<MutationResponse> =>
    request<MutationResponse>(`${workspacePath(workspaceId)}/issues${deferQuery(options)}`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  issueCommand: (
    workspaceId: string,
    issueId: string,
    body: IssueCommandRequest,
    options: CommandOptions = {},
  ): Promise<MutationResponse> =>
    request<MutationResponse>(
      `${workspacePath(workspaceId)}/issues/${encodeURIComponent(issueId)}/commands${deferQuery(
        options,
      )}`,
      { method: "POST", body: JSON.stringify(body) },
    ),

  issueDetail: (workspaceId: string, issueId: string): Promise<IssueDetail> =>
    request<IssueDetail>(`${workspacePath(workspaceId)}/issues/${encodeURIComponent(issueId)}`),

  /** Read-only lineage probe for one accepted acknowledgement. */
  coverage: (workspaceId: string, issueId: string, position: string): Promise<CoverageResponse> =>
    request<CoverageResponse>(
      `${workspacePath(workspaceId)}/issues/${encodeURIComponent(
        issueId,
      )}/coverage?position=${encodeURIComponent(position)}`,
    ),
};

/** Identifiers must satisfy the server's stream-segment rule. */
export function newId(prefix: string): string {
  const random = globalThis.crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  return `${prefix}${random}`;
}
