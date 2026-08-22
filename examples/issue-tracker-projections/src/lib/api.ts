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
import { Data } from "effect";

/**
 * A failed API call.
 *
 * The browser client is ordinary Promise code, so this is thrown and caught
 * with `instanceof` rather than carried in an Effect failure channel. It is a
 * tagged error so the failure is distinguishable by `_tag` instead of by class
 * identity alone; `name`, `message`, `status`, and `instanceof` behaviour are
 * unchanged.
 */
export class ApiFailure extends Data.TaggedError("ApiFailure")<{
  readonly message: string;
  readonly status: number;
}> {}

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
    throw new ApiFailure({
      message: error instanceof Error ? error.message : "Network error",
      status: 0,
    });
  }
  const text = await response.text();
  if (!response.ok) throw new ApiFailure({ message: errorDetail(text), status: response.status });
  return parseBody(text);
}

/**
 * Describe a failed response.
 *
 * The server reports failures as `ApiError`, but an error body can also be a
 * proxy page or an empty string, so the shape is checked before it is read.
 * Anything else stays as raw text.
 */
function errorDetail(text: string): string {
  const raw = text.slice(0, 200);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    // Non-JSON error bodies stay as raw text.
    return raw;
  }
  if (!isApiError(parsed)) return raw;
  const { error, detail } = parsed;
  return detail === undefined || detail.length === 0 ? error : `${error}: ${detail}`;
}

function isApiError(value: unknown): value is ApiError {
  if (typeof value !== "object" || value === null) return false;
  const error: unknown = Reflect.get(value, "error");
  const detail: unknown = Reflect.get(value, "detail");
  return typeof error === "string" && (detail === undefined || typeof detail === "string");
}

/**
 * Read a success body.
 *
 * The declared response type is the endpoint's wire contract, which the server
 * builds from the shared Schemas in `shared/requests.ts` and `shared/api.ts`.
 * The browser is a same-origin reader of its own API and does not re-validate
 * that contract. An empty body reads as `undefined`, which only the endpoints
 * declared as `Promise<unknown>` return.
 */
function parseBody(text: string) {
  return text.length === 0 ? undefined : JSON.parse(text);
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
