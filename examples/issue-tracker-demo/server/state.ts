import { DurableState } from "@streamsy/core";
import { Effect, Schema } from "effect";
import type { Comment, Issue, Project } from "../shared/state-schema.ts";
import type { WorkspaceChange } from "./streams.ts";

/** Read-only view over one workspace's materialized state. */
export interface WorkspaceState {
  getProject(projectId: string): Project | undefined;
  getIssue(issueId: string): Issue | undefined;
  hasProjects(): boolean;
}

/**
 * Server-chosen values for one write: a fresh entity id, the time and the
 * txid. A request makes one stamp, so every CAS retry writes the same values.
 */
export interface Stamp {
  readonly id: string;
  readonly now: string;
  readonly txid: string;
}

/** One validated write: the change to append and the value to answer with. */
export interface Mutation<A> {
  readonly change: WorkspaceChange;
  readonly value: A;
}

export class UnknownReference extends Schema.TaggedError<UnknownReference>()("UnknownReference", {
  field: Schema.Literals(["projectId", "issueId"]),
  id: Schema.String,
}) {}

export class IssueNotFound extends Schema.TaggedError<IssueNotFound>()("IssueNotFound", {
  issueId: Schema.String,
}) {}

const headers = (stamp: Stamp) => ({ headers: { timestamp: stamp.now, txid: stamp.txid } });

export const projectUpsert = (project: Project, stamp: Stamp): WorkspaceChange =>
  DurableState.upsert("project", project, headers(stamp));

export const issueUpsert = (issue: Issue, stamp: Stamp): WorkspaceChange =>
  DurableState.upsert("issue", issue, headers(stamp));

export const commentUpsert = (comment: Comment, stamp: Stamp): WorkspaceChange =>
  DurableState.upsert("comment", comment, headers(stamp));

// The attempts below run inside the read, fold, append recipe of
// `IssueTracker.mutate`. Each one validates the request against freshly folded
// state and either returns one change or fails with a typed error. They are
// pure, so a retry simply runs them again.

export const createProject =
  (input: Partial<Project>, stamp: Stamp) => (): Effect.Effect<Mutation<Project>> => {
    const project = newProject(input, stamp);
    return Effect.succeed({ change: projectUpsert(project, stamp), value: project });
  };

export const createIssue =
  (input: Partial<Issue>, stamp: Stamp) =>
  (state: WorkspaceState): Effect.Effect<Mutation<Issue>, UnknownReference> => {
    const issue = newIssue(input, stamp);
    if (!state.getProject(issue.projectId)) {
      return Effect.fail(new UnknownReference({ field: "projectId", id: issue.projectId }));
    }
    return Effect.succeed({ change: issueUpsert(issue, stamp), value: issue });
  };

export const updateIssue =
  (issueId: string, input: Partial<Issue>, stamp: Stamp) =>
  (state: WorkspaceState): Effect.Effect<Mutation<Issue>, IssueNotFound> => {
    const previous = state.getIssue(issueId);
    if (!previous) return Effect.fail(new IssueNotFound({ issueId }));
    const issue = nextIssue(previous, input, stamp);
    return Effect.succeed({ change: issueUpsert(issue, stamp), value: issue });
  };

export const createComment =
  (input: Partial<Comment>, stamp: Stamp) =>
  (state: WorkspaceState): Effect.Effect<Mutation<Comment>, UnknownReference> => {
    const comment = newComment(input, stamp);
    if (!state.getIssue(comment.issueId)) {
      return Effect.fail(new UnknownReference({ field: "issueId", id: comment.issueId }));
    }
    return Effect.succeed({ change: commentUpsert(comment, stamp), value: comment });
  };

export function newProject(input: Partial<Project>, stamp: Stamp): Project {
  return {
    id: input.id ?? stamp.id,
    name: (input.name ?? "Untitled project").trim(),
    description: (input.description ?? "").trim(),
    createdAt: input.createdAt ?? stamp.now,
  };
}

export function newIssue(input: Partial<Issue>, stamp: Stamp): Issue {
  const createdAt = input.createdAt ?? stamp.now;
  return {
    id: input.id ?? stamp.id,
    projectId: input.projectId ?? "",
    title: (input.title ?? "Untitled issue").trim(),
    status: input.status ?? "open",
    createdAt,
    updatedAt: input.updatedAt ?? createdAt,
  };
}

export function nextIssue(previous: Issue, input: Partial<Issue>, stamp: Stamp): Issue {
  return {
    ...previous,
    title: input.title === undefined ? previous.title : input.title.trim(),
    status: input.status ?? previous.status,
    updatedAt: input.updatedAt ?? stamp.now,
  };
}

export function newComment(input: Partial<Comment>, stamp: Stamp): Comment {
  return {
    id: input.id ?? stamp.id,
    issueId: input.issueId ?? "",
    author: (input.author ?? "you").trim() || "you",
    body: (input.body ?? "").trim(),
    createdAt: input.createdAt ?? stamp.now,
  };
}

/** The first project of a new shared workspace. */
export function starterProject(stamp: Stamp): Project {
  return {
    id: stamp.id,
    name: "Getting started",
    description: "Shared workspace — anyone with this link sees changes live.",
    createdAt: stamp.now,
  };
}

/** The known demo workspace's first changes. */
export function mainWorkspaceSeed(stamp: Stamp): WorkspaceChange[] {
  const createdAt = stamp.now;
  const projects: Project[] = [
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
  const issues: Issue[] = [
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
  const comments: Comment[] = [
    {
      id: "comment_welcome",
      issueId: "issue_bootstrap",
      author: "demo-server",
      body: "The app consumes JSON state events from /streams/workspace/main with @durable-streams/state StreamDB.",
      createdAt,
    },
  ];
  return [
    ...projects.map((project) => projectUpsert(project, stamp)),
    ...issues.map((issue) => issueUpsert(issue, stamp)),
    ...comments.map((comment) => commentUpsert(comment, stamp)),
  ];
}
