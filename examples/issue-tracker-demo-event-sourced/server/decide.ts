import {
  anonymousAuthor,
  wipLimit,
  type Comment,
  type Issue,
  type Project,
} from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Array, Effect, Schema } from "effect";
import { countInProgress, findIssue, hasProject, type CommandView } from "./command-view.ts";
import {
  commentRow,
  issueAfter,
  projectRow,
  WorkspaceEvent,
  type IssueCreated,
  type IssueEvent,
} from "./events.ts";

/**
 * Server-chosen values for one write: a fresh entity id, the time and the
 * txid. A request makes one stamp, so every CAS retry appends the same events.
 */
export interface Stamp {
  readonly id: string;
  readonly now: string;
  readonly txid: string;
}

/**
 * One accepted command: the events to append and the value to answer with. A
 * user command always has events. The seed has none when `main` has projects.
 */
export interface Decision<A> {
  readonly events: ReadonlyArray<WorkspaceEvent>;
  readonly value: A;
}

export class UnknownReference extends Schema.TaggedError<
  UnknownReference,
  { readonly brand: unique symbol }
>()("UnknownReference", {
  field: Schema.Literals(["projectId", "issueId"]),
  id: Schema.String,
}) {}

export class IssueNotFound extends Schema.TaggedError<
  IssueNotFound,
  { readonly brand: unique symbol }
>()("IssueNotFound", {
  issueId: Schema.String,
}) {}

/** The project already has `limit` issues in progress. */
export class TooManyInProgress extends Schema.TaggedError<
  TooManyInProgress,
  { readonly brand: unique symbol }
>()("TooManyInProgress", { projectId: Schema.String, limit: Schema.Finite }) {}

/**
 * A patch with no title and no status names no event. The base app would still
 * write the row. Here a write must append at least one event, because the
 * browser waits for its txid.
 */
export class NothingToChange extends Schema.TaggedError<
  NothingToChange,
  { readonly brand: unique symbol }
>()("NothingToChange", {
  issueId: Schema.String,
}) {}

const { ProjectCreated, IssueCreated, IssueRenamed, IssueStatusChanged, CommentAdded } =
  WorkspaceEvent.cases;

// The decisions below run inside `IssueTracker.mutate`, against the command
// view at the head of the event stream. Each one checks the request and either
// returns the events to append or fails with a typed error. They are pure, so
// a retry runs them again on the new view.

export const createProject =
  (input: Partial<Project>, stamp: Stamp) => (): Effect.Effect<Decision<Project>> => {
    const created = ProjectCreated.make({
      projectId: input.id ?? stamp.id,
      name: (input.name ?? "Untitled project").trim(),
      description: (input.description ?? "").trim(),
      at: stamp.now,
      txid: stamp.txid,
    });

    return Effect.succeed({ events: [created], value: projectRow(created) });
  };

export const createIssue =
  (input: Partial<Issue>, stamp: Stamp) =>
  (view: CommandView): Effect.Effect<Decision<Issue>, UnknownReference | TooManyInProgress> => {
    const created = issueCreated(input, stamp);

    if (!hasProject(view, created.projectId)) {
      return Effect.fail(new UnknownReference({ field: "projectId", id: created.projectId }));
    }

    if (startsWork(view, undefined, created.status, created.projectId)) {
      return Effect.fail(new TooManyInProgress({ projectId: created.projectId, limit: wipLimit }));
    }

    return decided([created], undefined);
  };

/**
 * A patch with a title appends `IssueRenamed`. A patch with a status appends
 * `IssueStatusChanged`. A patch with both appends both events in one append.
 */
export const updateIssue =
  (issueId: string, input: Partial<Issue>, stamp: Stamp) =>
  (
    view: CommandView,
  ): Effect.Effect<Decision<Issue>, IssueNotFound | TooManyInProgress | NothingToChange> => {
    const previous = findIssue(view, issueId);

    if (!previous) return Effect.fail(new IssueNotFound({ issueId }));
    const edit = { issueId, ...writer(input.updatedBy), at: stamp.now, txid: stamp.txid };
    const events: IssueEvent[] = [];

    if (input.title !== undefined) {
      events.push(IssueRenamed.make({ ...edit, title: input.title.trim() }));
    }

    if (input.status !== undefined) {
      if (startsWork(view, previous, input.status, previous.projectId)) {
        return Effect.fail(
          new TooManyInProgress({ projectId: previous.projectId, limit: wipLimit }),
        );
      }

      events.push(IssueStatusChanged.make({ ...edit, status: input.status }));
    }

    const [first, ...rest] = events;

    if (first === undefined) return Effect.fail(new NothingToChange({ issueId }));

    return decided([first, ...rest], previous);
  };

export const createComment =
  (input: Partial<Comment>, stamp: Stamp) =>
  (view: CommandView): Effect.Effect<Decision<Comment>, UnknownReference> => {
    const added = CommentAdded.make({
      commentId: input.id ?? stamp.id,
      issueId: input.issueId ?? "",
      author: input.author?.trim() || anonymousAuthor,
      body: (input.body ?? "").trim(),
      at: stamp.now,
      txid: stamp.txid,
    });

    if (!findIssue(view, added.issueId)) {
      return Effect.fail(new UnknownReference({ field: "issueId", id: added.issueId }));
    }

    return Effect.succeed({ events: [added], value: commentRow(added) });
  };

/**
 * The work-in-progress rule, as in the base app. It reads every issue of the
 * project, so it holds only because each decision sees the view at the head
 * that its events append to. An issue that stays in progress does not count
 * twice.
 */
function startsWork(
  view: CommandView,
  previous: Issue | undefined,
  status: Issue["status"],
  projectId: string,
): boolean {
  return (
    status === "in_progress" &&
    previous?.status !== "in_progress" &&
    countInProgress(view, projectId) >= wipLimit
  );
}

/** The answer is the issue row after the events, as the read model writes it. */
function decided(
  events: Array.NonEmptyReadonlyArray<IssueEvent>,
  previous: Issue | undefined,
): Effect.Effect<Decision<Issue>> {
  const issue = Array.reduce<IssueEvent, Issue | undefined>(events, previous, issueAfter);

  return issue === undefined
    ? Effect.die(new Error(`An issue decision gave no row for ${events[0].issueId}`))
    : Effect.succeed({ events, value: issue });
}

// The server owns time: every time field comes from the stamp, never from
// the request.

function issueCreated(input: Partial<Issue>, stamp: Stamp): IssueCreated {
  return IssueCreated.make({
    issueId: input.id ?? stamp.id,
    projectId: input.projectId ?? "",
    title: (input.title ?? "Untitled issue").trim(),
    status: input.status ?? "open",
    ...writer(input.updatedBy),
    at: stamp.now,
    txid: stamp.txid,
  });
}

/** A blank display name is no name. */
function writer(name: string | undefined): { readonly updatedBy?: string } {
  const trimmed = name?.trim();

  return trimmed ? { updatedBy: trimmed } : {};
}

/** The first project of a new shared workspace. */
export function starterProject(stamp: Stamp): Partial<Project> {
  return {
    id: stamp.id,
    name: "Getting started",
    description: "Shared workspace — anyone with this link sees changes live.",
  };
}

/**
 * The known demo workspace's first events: the base app's seed rows, as the
 * events that create them. "Streamsy Demo" starts with two issues in progress,
 * one below the limit, so two tabs can race for the last place.
 */
export function mainWorkspaceSeed(stamp: Stamp): Array.NonEmptyReadonlyArray<WorkspaceEvent> {
  const stamped = { at: stamp.now, txid: stamp.txid };

  const issue = (
    issueId: string,
    projectId: string,
    title: string,
    status: Issue["status"],
  ): WorkspaceEvent => IssueCreated.make({ issueId, projectId, title, status, ...stamped });

  return [
    ProjectCreated.make({
      projectId: "proj_streamsy",
      name: "Streamsy Demo",
      description: "A tiny issue tracker synced over durable streams.",
      ...stamped,
    }),
    ProjectCreated.make({
      projectId: "proj_docs",
      name: "Writing",
      description: "Article examples and docs follow-ups.",
      ...stamped,
    }),
    issue(
      "issue_bootstrap",
      "proj_streamsy",
      "Hydrate TanStack DB directly from the durable stream",
      "done",
    ),
    issue(
      "issue_optimistic",
      "proj_streamsy",
      "Use StreamDB collections for optimistic local writes",
      "in_progress",
    ),
    issue(
      "issue_filters",
      "proj_streamsy",
      "Filter issues by status with live queries",
      "in_progress",
    ),
    issue("issue_names", "proj_streamsy", "Show who made each change", "open"),
    issue("issue_article", "proj_docs", "Connect the demo to the Part 2 StreamDB article", "open"),
    CommentAdded.make({
      commentId: "comment_welcome",
      issueId: "issue_bootstrap",
      author: "demo-server",
      body: "The app consumes JSON state events from /streams/workspace/main with @durable-streams/state StreamDB.",
      ...stamped,
    }),
  ];
}
