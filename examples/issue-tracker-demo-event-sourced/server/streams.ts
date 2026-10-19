import { MemoryStorage, Streamsy } from "@streamsy/core";
import { Comment, Issue, Project } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { CheckpointStore } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { Effect, Layer, Option, Schema } from "effect";
import { ServerConfig } from "./config.ts";
import { WorkspaceEvent } from "./events.ts";

/** The source of truth: the domain events of one workspace. The browser never reads it. */
export class WorkspaceEvents extends Streamsy.json<WorkspaceEvents>()(
  "IssueTrackerEventSourced/WorkspaceEvents",
  "workspace-events/:workspaceId",
  { params: { workspaceId: Schema.String }, schema: WorkspaceEvent },
) {}

/**
 * The read model at the base app's public path. Same collections, same rows.
 * Only the read model projection appends to it.
 */
export class Workspaces extends Streamsy.state<Workspaces>()(
  "IssueTrackerEventSourced/Workspaces",
  "workspace/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    collections: {
      project: { schema: Project, key: "id" },
      issue: { schema: Issue, key: "id" },
      comment: { schema: Comment, key: "id" },
    },
  },
) {}

/** One record for each shared workspace, so a restart knows which projections to start. */
export const RegisteredWorkspace = Schema.Struct({
  workspaceId: Schema.String,
  createdAt: Schema.String,
});

export class WorkspaceRegistry extends Streamsy.json<WorkspaceRegistry>()(
  "IssueTrackerEventSourced/WorkspaceRegistry",
  "workspace-registry",
  { params: {}, schema: RegisteredWorkspace },
) {}

export const streams = Streamsy.group(WorkspaceEvents, Workspaces, WorkspaceRegistry);

/**
 * SQLite when `ISSUE_TRACKER_DB` is set, otherwise a process-local memory store.
 * `CheckpointStore.layer` keeps the projection checkpoints on the same owner, so
 * a checkpoint and the State rows it wrote commit together, and a restart with
 * a database path resumes each projection where it stopped.
 */
export const StorageLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ databasePath }) =>
    CheckpointStore.layer.pipe(Layer.provideMerge(storage(databasePath))),
  ),
);

function storage(databasePath: Option.Option<string>) {
  return Option.match(databasePath, {
    onNone: () => MemoryStorage.layer(streams),
    onSome: (filename) => BunStorage.layer(streams, { client: { filename } }),
  });
}
