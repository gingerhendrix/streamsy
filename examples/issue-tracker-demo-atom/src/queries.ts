import { Effect, Option, Stream } from "effect";
import { AsyncResult, Atom } from "effect/reactivity";
import {
  applyReadEvent,
  commentsByIssue,
  emptyReplica,
  issueCountByProject,
  issuesIn,
  projectsOldestFirst,
  selectedProject,
  statusCounts,
  unconfirmed,
  visibleIssues,
  workspaceView,
} from "./domain/workspace.ts";
import { atomRuntime, WorkspaceReader } from "./runtime.ts";
import { filterAtom, localWritesAtom, selectedProjectIdAtom, workspaceIdAtom } from "./state.ts";

export type { StatusFilter } from "./domain/workspace.ts";

// Every value the page derives from the workspace is an atom. The registry
// recomputes an atom only when a value it read changes, so a new filter sorts
// the visible issues again and leaves the project list alone.

/** The stream read, folded. `keepAlive` holds the read open for the life of the page. */
const replicaResultAtom = Atom.keepAlive(
  atomRuntime.atom((get) => {
    const workspaceId = get(workspaceIdAtom);

    return Stream.unwrap(
      WorkspaceReader.use((reader) => Effect.succeed(reader.read(workspaceId))),
    ).pipe(Stream.scan(() => emptyReplica, applyReadEvent));
  }),
);

export const replicaAtom = Atom.map(
  replicaResultAtom,
  AsyncResult.getOrElse(() => emptyReplica),
);

/**
 * The writes that are waiting for their txid. No code removes a confirmed
 * write from a list: it stops being pending when its txid is in the replica.
 */
export const pendingWritesAtom = Atom.make((get) =>
  unconfirmed(get(localWritesAtom), get(replicaAtom).confirmed),
);

/** The stream rows with every pending write on top. */
export const workspaceAtom = Atom.make((get) =>
  workspaceView(get(replicaAtom).rows, get(pendingWritesAtom)),
);

/** All projects, oldest first. Projects with the same time sort by name. */
export const projectsAtom = Atom.map(workspaceAtom, projectsOldestFirst);

/** The selected project, or the first project when nothing valid is selected. */
export const selectedProjectAtom = Atom.make((get) =>
  selectedProject(get(projectsAtom), get(selectedProjectIdAtom)),
);

const selectedIssuesAtom = Atom.make((get) => {
  const workspace = get(workspaceAtom);

  return Option.match(get(selectedProjectAtom), {
    onNone: () => [],
    onSome: (project) => issuesIn(workspace, project.id),
  });
});

/** The issues of the selected project that pass the filter, last updated first. */
export const visibleIssuesAtom = Atom.make((get) =>
  visibleIssues(get(selectedIssuesAtom), get(filterAtom)),
);

/** How many issues of the selected project have each status. */
export const statusCountsAtom = Atom.map(selectedIssuesAtom, statusCounts);

/** The issue count of each project, by project id. */
export const issueCountByProjectAtom = Atom.map(workspaceAtom, issueCountByProject);

/** Comments grouped by issue, oldest first in each group. */
export const commentsByIssueAtom = Atom.map(workspaceAtom, commentsByIssue);
