/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/global-fetch -- This Promise-native browser client owns Web fetch and WebCrypto platform boundaries. */
/** Typed browser client for the demo API. Every failure surfaces a message. */
import { Option, Schema } from "effect";
import {
  ApiError,
  BoardResponse,
  CoverageResponse,
  HealthResponse,
  MutationResponse,
  ProjectsResponse,
} from "../../shared/api.ts";
import type {
  CreateIssueRequest,
  CreateProjectRequest,
  IssueCommandRequest,
} from "../../shared/api.ts";
import {
  IssueDetailSchema,
  ProjectSchema,
  type IssueDetail,
  type Project,
} from "../../shared/model.ts";

/**
 * A failed API call.
 *
 * This client is Promise-native browser code: it is thrown and caught with
 * `instanceof`, never carried in an Effect failure channel. Importing Effect to
 * obtain a tagged error would pull the Effect runtime into the browser bundle
 * for no behavioural gain, so the native subclass stays and the Effect rule is
 * suppressed on this line alone.
 */
// oxlint-disable-next-line effecttsgo/extends-native-error -- ApiFailure is a Promise-native browser client error; importing Effect would add runtime code to the browser bundle.
export class ApiFailure extends Error {
  constructor(
    override readonly message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiFailure";
  }
}

async function request<T>(schema: Schema.Decoder<T>, path: string, init?: RequestInit): Promise<T> {
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
  if (!response.ok) throw new ApiFailure(errorDetail(text), response.status);
  return Schema.decodeUnknownSync(schema)(parseBody(text));
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
  const decoded = Schema.decodeUnknownOption(ApiError)(parsed);
  if (Option.isNone(decoded)) return raw;
  const { error, detail } = decoded.value;
  return detail === undefined || detail.length === 0 ? error : `${error}: ${detail}`;
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
  health: (): Promise<HealthResponse> => request(HealthResponse, "/health"),

  seed: (workspaceId: string): Promise<unknown> =>
    request(Schema.Unknown, `${workspacePath(workspaceId)}/seed`, { method: "POST" }),

  listProjects: (workspaceId: string): Promise<readonly Project[]> =>
    request(ProjectsResponse, `${workspacePath(workspaceId)}/projects`).then(
      (response) => response.projects,
    ),

  createProject: (workspaceId: string, body: CreateProjectRequest): Promise<Project> =>
    request(ProjectSchema, `${workspacePath(workspaceId)}/projects`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  board: (workspaceId: string, projectId: string): Promise<BoardResponse> =>
    request(
      BoardResponse,
      `${workspacePath(workspaceId)}/projects/${encodeURIComponent(projectId)}/board`,
    ),

  repair: (workspaceId: string, projectId: string): Promise<unknown> =>
    request(
      Schema.Unknown,
      `${workspacePath(workspaceId)}/projects/${encodeURIComponent(projectId)}/repair`,
      {
        method: "POST",
      },
    ),

  createIssue: (
    workspaceId: string,
    body: CreateIssueRequest,
    options: CommandOptions = {},
  ): Promise<MutationResponse> =>
    request(MutationResponse, `${workspacePath(workspaceId)}/issues${deferQuery(options)}`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  issueCommand: (
    workspaceId: string,
    issueId: string,
    body: IssueCommandRequest,
    options: CommandOptions = {},
  ): Promise<MutationResponse> =>
    request(
      MutationResponse,
      `${workspacePath(workspaceId)}/issues/${encodeURIComponent(issueId)}/commands${deferQuery(
        options,
      )}`,
      { method: "POST", body: JSON.stringify(body) },
    ),

  issueDetail: (workspaceId: string, issueId: string): Promise<IssueDetail> =>
    request(
      IssueDetailSchema,
      `${workspacePath(workspaceId)}/issues/${encodeURIComponent(issueId)}`,
    ),

  /** Read-only lineage probe for one accepted acknowledgement. */
  coverage: (workspaceId: string, issueId: string, position: string): Promise<CoverageResponse> =>
    request(
      CoverageResponse,
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
