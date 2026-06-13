import { MaterializedState } from "@durable-streams/state";
import { ZERO_OFFSET } from "@streamsy/core";
import {
  issueTrackerState,
  type Comment,
  type Issue,
  type Project,
  type StateEvent,
} from "../shared/state-schema.ts";
import { mainWorkspaceId, workspaceStreamId } from "./config.ts";
import type { DemoStreams } from "./streams.ts";
import { conflict, id, notFound, now, type TxId } from "./utils.ts";

/** Read-only view over one workspace's materialized state. */
export interface WorkspaceState {
  getProject(projectId: string): Project | undefined;
  getIssue(issueId: string): Issue | undefined;
  hasProjects(): boolean;
}

function eventHeaders(txid: TxId = crypto.randomUUID()) {
  return {
    timestamp: now(),
    txid,
  };
}

/**
 * Materialize one workspace's state by folding its durable stream from the
 * beginning. Stateless by design: nothing outlives the request. Returns the
 * state plus the stream's head offset — the CAS token for appends conditioned
 * on exactly this state — or undefined when the workspace does not exist.
 */
export async function materializeWorkspace(
  streams: DemoStreams,
  workspaceId: string,
): Promise<{ state: WorkspaceState; headOffset: string } | undefined> {
  const read = await streams.readAll(workspaceStreamId(workspaceId));
  if (!read) return undefined;

  const state = new MaterializedState();
  for (const event of read.events) {
    state.apply(event);
  }

  return {
    state: {
      getProject: (projectId) => state.get<Project>("project", projectId),
      getIssue: (issueId) => state.get<Issue>("issue", issueId),
      hasProjects: () => state.getType("project").size > 0,
    },
    headOffset: read.headOffset,
  };
}

/** Outcome of one mutation attempt against freshly materialized state. */
export type MutationAttempt =
  | { response: Response }
  | { event: StateEvent; respond: (ack: { offset: string }) => Response };

const maxMutationAttempts = 4;

/**
 * Optimistic-concurrency mutation loop (CAS append with retry): materialize
 * the workspace, let the caller validate and build an event from that fresh
 * state, then append conditioned on the materialized head offset. If another
 * writer appended in between, re-materialize and retry — validation and the
 * event are rebuilt from the new state each attempt, so a successful CAS
 * proves validation ran against the exact state the event landed on. Correct
 * even across processes; production writers would add backoff/jitter.
 *
 * Validation outcomes (`{ response }`) are returned as-is and never retried.
 * Unknown workspaces yield 404; retry exhaustion yields 409.
 */
export async function mutateWorkspace(
  streams: DemoStreams,
  workspaceId: string,
  attempt: (state: WorkspaceState) => MutationAttempt,
): Promise<Response> {
  const streamId = workspaceStreamId(workspaceId);

  for (let attemptNumber = 1; attemptNumber <= maxMutationAttempts; attemptNumber++) {
    const materialized = await materializeWorkspace(streams, workspaceId);
    if (!materialized) return notFound("Unknown workspace");

    const outcome = attempt(materialized.state);
    if ("response" in outcome) return outcome.response;

    const result = await streams.appendEvent(streamId, outcome.event, materialized.headOffset);
    if (result.status === "appended") {
      return outcome.respond({ offset: result.offset });
    }
    if (result.status === "conflict" && result.conflictReason === "expected-offset") {
      continue; // Lost the race; re-materialize and rebuild from the new head.
    }
    throw new Error(`Unable to append state event to ${streamId}: ${result.status}`);
  }

  return conflict("Concurrent updates, please retry");
}

// === Event builders ===

export function projectUpsert(project: Project, txid?: TxId): StateEvent {
  return issueTrackerState.projects.upsert({
    value: project,
    headers: eventHeaders(txid),
  });
}

export function issueUpsert(issue: Issue, txid?: TxId): StateEvent {
  return issueTrackerState.issues.upsert({
    value: issue,
    headers: eventHeaders(txid),
  });
}

export function issueUpdate(issue: Issue, oldValue: Issue, txid?: TxId): StateEvent {
  return issueTrackerState.issues.update({
    value: issue,
    oldValue,
    headers: eventHeaders(txid),
  });
}

export function commentUpsert(comment: Comment, txid?: TxId): StateEvent {
  return issueTrackerState.comments.upsert({
    value: comment,
    headers: eventHeaders(txid),
  });
}

// === Seeding ===

async function appendSeedEvent(
  streams: DemoStreams,
  streamId: string,
  event: StateEvent,
  expectedOffset?: string,
): Promise<void> {
  const result = await streams.appendEvent(streamId, event, expectedOffset);
  if (result.status !== "appended") {
    throw new Error(`Unable to seed Streamsy stream ${streamId}: ${result.status}`);
  }
}

/**
 * Ensure and seed the known demo workspace at boot. No-op when the stream
 * already holds projects (e.g. a hot reload of the server module).
 */
export async function seedMainWorkspace(streams: DemoStreams): Promise<void> {
  const streamId = workspaceStreamId(mainWorkspaceId);
  await streams.ensureStream(streamId);

  const materialized = await materializeWorkspace(streams, mainWorkspaceId);
  if (!materialized || materialized.state.hasProjects()) return;

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
      body: "The app consumes JSON state events from /streams/workspace/main with @durable-streams/state StreamDB.",
      createdAt,
    },
  ];

  const events: StateEvent[] = [
    ...initialProjects.map((project) => projectUpsert(project)),
    ...initialIssues.map((issue) => issueUpsert(issue)),
    ...initialComments.map((comment) => commentUpsert(comment)),
  ];
  for (const event of events) {
    await appendSeedEvent(streams, streamId, event);
  }
}

/**
 * Seed a freshly created shared workspace with one starter project so the
 * shareable link does not open onto a blank UI. The `ZERO_OFFSET` CAS is a
 * belt-and-braces assertion that the new stream is still empty.
 */
export async function seedStarterProject(streams: DemoStreams, workspaceId: string): Promise<void> {
  const project: Project = {
    id: id("proj"),
    name: "Getting started",
    description: "Shared workspace — anyone with this link sees changes live.",
    createdAt: now(),
  };
  await appendSeedEvent(
    streams,
    workspaceStreamId(workspaceId),
    projectUpsert(project),
    ZERO_OFFSET,
  );
}

// === Entity builders ===

export function newProject(input: Partial<Project>): Project {
  return {
    id: input.id ?? id("proj"),
    name: String(input.name ?? "Untitled project").trim(),
    description: String(input.description ?? "").trim(),
    createdAt: input.createdAt ?? now(),
  };
}

export function newIssue(input: Partial<Issue>): Issue {
  const timestamp = now();
  const createdAt = input.createdAt ?? timestamp;
  return {
    id: input.id ?? id("issue"),
    projectId: String(input.projectId ?? ""),
    title: String(input.title ?? "Untitled issue").trim(),
    status: input.status ?? "open",
    createdAt,
    updatedAt: input.updatedAt ?? createdAt,
  };
}

export function nextIssue(previous: Issue, input: Partial<Issue>): Issue {
  return {
    ...previous,
    title: input.title === undefined ? previous.title : String(input.title).trim(),
    status: input.status ?? previous.status,
    updatedAt: input.updatedAt ?? now(),
  };
}

export function newComment(input: Partial<Comment>): Comment {
  return {
    id: input.id ?? id("comment"),
    issueId: String(input.issueId ?? ""),
    author: String(input.author ?? "you").trim() || "you",
    body: String(input.body ?? "").trim(),
    createdAt: input.createdAt ?? now(),
  };
}
