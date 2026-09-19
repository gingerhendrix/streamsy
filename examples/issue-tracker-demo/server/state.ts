import { MaterializedState } from "@durable-streams/state";
import { Streams, ZERO_OFFSET } from "@streamsy/core";
import { Effect, Stream } from "effect";
import {
  issueTrackerState,
  type Comment,
  type Issue,
  type Project,
  type StateEvent,
} from "../shared/state-schema.ts";
import { mainWorkspaceId } from "./config.ts";
import { appendWorkspaceEvent, workspaceEvents } from "./streams.ts";
import { conflict, id, notFound, now, type TxId } from "./utils.ts";

/** Read-only view over one workspace's materialized state. */
export interface WorkspaceState {
  getProject(projectId: string): Project | undefined;
  getIssue(issueId: string): Issue | undefined;
  hasProjects(): boolean;
}

function eventHeaders(txid: TxId = crypto.randomUUID()) {
  return { timestamp: now(), txid };
}

/**
 * Fold one workspace from its durable stream. No materialized state survives
 * the request; the returned head is the CAS token for the exact folded view.
 */
export const materializeWorkspace = Effect.fn("Workspace.materialize")(function* (
  workspaceId: string,
) {
  const batches = yield* Streams.read(workspaceEvents(workspaceId)).pipe(Stream.runCollect);
  const state = new MaterializedState();
  for (const batch of batches) for (const event of batch.items) state.apply(event);
  return {
    state: {
      getProject: (projectId: string) => state.get<Project>("project", projectId),
      getIssue: (issueId: string) => state.get<Issue>("issue", issueId),
      hasProjects: () => state.getType("project").size > 0,
    } satisfies WorkspaceState,
    headOffset: batches.at(-1)?.nextOffset ?? ZERO_OFFSET,
  };
});

/** Outcome of one mutation attempt against freshly materialized state. */
export type MutationAttempt =
  | { response: Response }
  | { event: StateEvent; respond: (ack: { offset: string }) => Response };

/**
 * The Transact recipe: read, fold, append with `expectedOffset`, and retry
 * only an `OffsetMismatch`. Each retry rebuilds validation and the event from
 * the newly folded state, preventing a lost update across concurrent writers.
 */
export const mutateWorkspace = (
  workspaceId: string,
  attempt: (state: WorkspaceState) => MutationAttempt,
) =>
  Effect.gen(function* () {
    const materialized = yield* materializeWorkspace(workspaceId).pipe(
      Effect.map((value) => ({ found: true as const, value })),
      Effect.catchTags({
        StreamNotFound: () => Effect.succeed({ found: false as const }),
        StreamGone: () => Effect.succeed({ found: false as const }),
      }),
    );
    if (!materialized.found) return notFound("Unknown workspace");

    const outcome = attempt(materialized.value.state);
    if ("response" in outcome) return outcome.response;

    const ack = yield* appendWorkspaceEvent(
      workspaceId,
      outcome.event,
      materialized.value.headOffset,
    );
    return outcome.respond({ offset: ack.offset });
  }).pipe(
    Effect.retry({ times: 3, while: (error) => error._tag === "OffsetMismatch" }),
    Effect.catchTag("OffsetMismatch", () =>
      Effect.succeed(conflict("Concurrent updates, please retry")),
    ),
  );

// These builders preserve the browser transaction id and timestamp. A later
// toolkit helper can replace them once custom header metadata is supported.
export function projectUpsert(project: Project, txid?: TxId): StateEvent {
  return issueTrackerState.projects.upsert({ value: project, headers: eventHeaders(txid) });
}

export function issueUpsert(issue: Issue, txid?: TxId): StateEvent {
  return issueTrackerState.issues.upsert({ value: issue, headers: eventHeaders(txid) });
}

export function commentUpsert(comment: Comment, txid?: TxId): StateEvent {
  return issueTrackerState.comments.upsert({ value: comment, headers: eventHeaders(txid) });
}

const appendSeedEvent = Effect.fn("Workspace.appendSeedEvent")(function* (
  workspaceId: string,
  event: StateEvent,
  expectedOffset?: string,
) {
  yield* appendWorkspaceEvent(workspaceId, event, expectedOffset);
});

/** Ensure and seed the known demo workspace at boot. */
export const seedMainWorkspace = Effect.fn("Workspace.seedMain")(function* () {
  yield* Streams.create(workspaceEvents(mainWorkspaceId));
  const materialized = yield* materializeWorkspace(mainWorkspaceId);
  if (materialized.state.hasProjects()) return;

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
  for (const event of events) yield* appendSeedEvent(mainWorkspaceId, event);
});

/** Seed a new shared workspace with one starter project. */
export const seedStarterProject = Effect.fn("Workspace.seedStarter")(function* (
  workspaceId: string,
) {
  const project: Project = {
    id: id("proj"),
    name: "Getting started",
    description: "Shared workspace — anyone with this link sees changes live.",
    createdAt: now(),
  };
  yield* appendSeedEvent(workspaceId, projectUpsert(project), ZERO_OFFSET);
});

export function newProject(input: Partial<Project>): Project {
  return {
    id: input.id ?? id("proj"),
    name: (input.name ?? "Untitled project").trim(),
    description: (input.description ?? "").trim(),
    createdAt: input.createdAt ?? now(),
  };
}

export function newIssue(input: Partial<Issue>): Issue {
  const timestamp = now();
  const createdAt = input.createdAt ?? timestamp;
  return {
    id: input.id ?? id("issue"),
    projectId: input.projectId ?? "",
    title: (input.title ?? "Untitled issue").trim(),
    status: input.status ?? "open",
    createdAt,
    updatedAt: input.updatedAt ?? createdAt,
  };
}

export function nextIssue(previous: Issue, input: Partial<Issue>): Issue {
  return {
    ...previous,
    title: input.title === undefined ? previous.title : input.title.trim(),
    status: input.status ?? previous.status,
    updatedAt: input.updatedAt ?? now(),
  };
}

export function newComment(input: Partial<Comment>): Comment {
  return {
    id: input.id ?? id("comment"),
    issueId: input.issueId ?? "",
    author: (input.author ?? "you").trim() || "you",
    body: (input.body ?? "").trim(),
    createdAt: input.createdAt ?? now(),
  };
}
