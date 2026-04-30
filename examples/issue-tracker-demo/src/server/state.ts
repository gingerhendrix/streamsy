import { MaterializedState, type ChangeEvent } from "@durable-streams/state";
import { issueTrackerState, type Comment, type EntityByType, type EntityType, type Issue, type Project } from "../state-schema.ts";
import { id, now } from "./http.ts";
import type { DemoStreams } from "./streams.ts";

const state = new MaterializedState();

function values<T>(type: EntityType): T[] {
  return Array.from(state.getType(type).values()) as T[];
}

function eventHeaders() {
  return {
    timestamp: now(),
    txid: crypto.randomUUID(),
  };
}

export function projects(): Project[] {
  return values<Project>("project");
}

export function issues(): Issue[] {
  return values<Issue>("issue");
}

export function comments(): Comment[] {
  return values<Comment>("comment");
}

export function getProject(projectId: string): Project | undefined {
  return state.get<Project>("project", projectId);
}

export function getIssue(issueId: string): Issue | undefined {
  return state.get<Issue>("issue", issueId);
}

async function emit<T extends EntityType>(
  streams: DemoStreams,
  event: ChangeEvent<EntityByType[T]>,
): Promise<{ event: ChangeEvent<EntityByType[T]>; offset: string }> {
  const offset = await streams.appendJson(event);
  state.apply(event);
  return { event, offset };
}

export async function insertProject(streams: DemoStreams, project: Project) {
  return emit(streams, issueTrackerState.projects.upsert({ value: project, headers: eventHeaders() }));
}

export async function insertIssue(streams: DemoStreams, issue: Issue) {
  return emit(streams, issueTrackerState.issues.upsert({ value: issue, headers: eventHeaders() }));
}

export async function updateIssueState(streams: DemoStreams, issue: Issue, oldValue: Issue) {
  return emit(streams, issueTrackerState.issues.update({ value: issue, oldValue, headers: eventHeaders() }));
}

export async function insertComment(streams: DemoStreams, comment: Comment) {
  return emit(streams, issueTrackerState.comments.upsert({ value: comment, headers: eventHeaders() }));
}

export async function seed(streams: DemoStreams): Promise<void> {
  if (projects().length > 0) return;
  const createdAt = now();
  const initialProjects: Project[] = [
    {
      id: "proj_streamsy",
      name: "Streamsy Demo",
      description: "A tiny issue tracker synced over durable streams.",
      createdAt,
    },
    {
      id: "proj_docs",
      name: "Writing",
      description: "Article examples and docs follow-ups.",
      createdAt,
    },
  ];

  const initialIssues: Issue[] = [
    {
      id: "issue_bootstrap",
      projectId: "proj_streamsy",
      title: "Hydrate TanStack DB directly from the durable stream",
      status: "done",
      createdAt,
      updatedAt: createdAt,
    },
    {
      id: "issue_optimistic",
      projectId: "proj_streamsy",
      title: "Use StreamDB collections for optimistic local writes",
      status: "in_progress",
      createdAt,
      updatedAt: createdAt,
    },
    {
      id: "issue_article",
      projectId: "proj_docs",
      title: "Connect the demo to the Part 2 StreamDB article",
      status: "open",
      createdAt,
      updatedAt: createdAt,
    },
  ];

  const initialComments: Comment[] = [
    {
      id: "comment_welcome",
      issueId: "issue_bootstrap",
      author: "demo-server",
      body: "The app consumes JSON state events from /streams/session/main with @durable-streams/state StreamDB.",
      createdAt,
    },
  ];

  for (const project of initialProjects) await insertProject(streams, project);
  for (const issue of initialIssues) await insertIssue(streams, issue);
  for (const comment of initialComments) await insertComment(streams, comment);
}

export function newProject(input: Partial<Project>): Project {
  return {
    id: input.id ?? id("proj"),
    name: String(input.name ?? "Untitled project").trim(),
    description: String(input.description ?? "").trim(),
    createdAt: now(),
  };
}

export function newIssue(input: Partial<Issue>): Issue {
  const timestamp = now();
  return {
    id: input.id ?? id("issue"),
    projectId: String(input.projectId ?? ""),
    title: String(input.title ?? "Untitled issue").trim(),
    status: input.status ?? "open",
    createdAt: timestamp,
    updatedAt: timestamp,
  };
}

export function nextIssue(previous: Issue, input: Partial<Issue>): Issue {
  return {
    ...previous,
    title: input.title === undefined ? previous.title : String(input.title).trim(),
    status: input.status ?? previous.status,
    updatedAt: now(),
  };
}

export function newComment(input: Partial<Comment>): Comment {
  return {
    id: input.id ?? id("comment"),
    issueId: String(input.issueId ?? ""),
    author: String(input.author ?? "you").trim() || "you",
    body: String(input.body ?? "").trim(),
    createdAt: now(),
  };
}
