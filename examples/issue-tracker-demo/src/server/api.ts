import type { Comment, Issue, Project } from "../types.ts";
import { json } from "./http.ts";
import {
  getIssue,
  getProject,
  insertComment,
  insertIssue,
  insertProject,
  newComment,
  newIssue,
  newProject,
  nextIssue,
  updateIssueState,
} from "./state.ts";
import type { DemoStreams } from "./streams.ts";

type TxId = `${string}-${string}-${string}-${string}-${string}`;
type MutationBody<T> = Partial<T> & { txid?: TxId };

export function createApiRouter(streams: DemoStreams) {
  async function createProject(request: Request): Promise<Response> {
    const body = (await request.json()) as MutationBody<Project>;
    const project = newProject(body);
    const result = await insertProject(streams, project, body.txid);
    return json({ project, awaitOffset: result.offset, txid: result.event.headers.txid }, { status: 201 });
  }

  async function createIssue(request: Request): Promise<Response> {
    const body = (await request.json()) as MutationBody<Issue>;
    const issue = newIssue(body);
    if (!getProject(issue.projectId)) {
      return json({ error: "Unknown projectId" }, { status: 400 });
    }
    const result = await insertIssue(streams, issue, body.txid);
    return json({ issue, awaitOffset: result.offset, txid: result.event.headers.txid }, { status: 201 });
  }

  async function updateIssue(request: Request, issueId: string): Promise<Response> {
    const previous = getIssue(issueId);
    if (!previous) return json({ error: "Issue not found" }, { status: 404 });

    const body = (await request.json()) as MutationBody<Issue>;
    const issue = nextIssue(previous, body);
    const result = await updateIssueState(streams, issue, previous, body.txid);
    return json({ issue, awaitOffset: result.offset, txid: result.event.headers.txid });
  }

  async function createComment(request: Request): Promise<Response> {
    const body = (await request.json()) as MutationBody<Comment>;
    const comment = newComment(body);
    if (!getIssue(comment.issueId)) {
      return json({ error: "Unknown issueId" }, { status: 400 });
    }
    const result = await insertComment(streams, comment, body.txid);
    return json({ comment, awaitOffset: result.offset, txid: result.event.headers.txid }, { status: 201 });
  }

  return async function routeApi(request: Request, url: URL): Promise<Response> {
    if (url.pathname === "/api/projects" && request.method === "POST") {
      return createProject(request);
    }
    if (url.pathname === "/api/issues" && request.method === "POST") {
      return createIssue(request);
    }
    const issueMatch = url.pathname.match(/^\/api\/issues\/([^/]+)$/);
    if (issueMatch && request.method === "PATCH") {
      return updateIssue(request, decodeURIComponent(issueMatch[1]!));
    }
    if (url.pathname === "/api/comments" && request.method === "POST") {
      return createComment(request);
    }
    return json({ error: "Not found" }, { status: 404 });
  };
}
