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

export function createApiRouter(streams: DemoStreams) {
  async function createProject(request: Request): Promise<Response> {
    const project = newProject((await request.json()) as Partial<Project>);
    const result = await insertProject(streams, project);
    return json({ project, awaitOffset: result.offset }, { status: 201 });
  }

  async function createIssue(request: Request): Promise<Response> {
    const issue = newIssue((await request.json()) as Partial<Issue>);
    if (!getProject(issue.projectId)) {
      return json({ error: "Unknown projectId" }, { status: 400 });
    }
    const result = await insertIssue(streams, issue);
    return json({ issue, awaitOffset: result.offset }, { status: 201 });
  }

  async function updateIssue(request: Request, issueId: string): Promise<Response> {
    const previous = getIssue(issueId);
    if (!previous) return json({ error: "Issue not found" }, { status: 404 });

    const issue = nextIssue(previous, (await request.json()) as Partial<Issue>);
    const result = await updateIssueState(streams, issue, previous);
    return json({ issue, awaitOffset: result.offset });
  }

  async function createComment(request: Request): Promise<Response> {
    const comment = newComment((await request.json()) as Partial<Comment>);
    if (!getIssue(comment.issueId)) {
      return json({ error: "Unknown issueId" }, { status: 400 });
    }
    const result = await insertComment(streams, comment);
    return json({ comment, awaitOffset: result.offset }, { status: 201 });
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
