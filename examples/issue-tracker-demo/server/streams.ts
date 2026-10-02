import {
  DurableState,
  MemoryStorage,
  Streamsy,
  type StorageFault,
  type StreamAddress,
} from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { type Array, Effect, Layer, Option, Schema } from "effect";
import { Comment, Issue, Project } from "../shared/state-schema.ts";
import { ServerConfig } from "./config.ts";

/** One stream owns every workspace's multi-collection stream. */
export class Workspaces extends Streamsy.state<Workspaces>()(
  "IssueTrackerDemo/Workspaces",
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

export type WorkspaceChange = DurableState.Change<typeof Workspaces.collections>;
/** One stamped event on a workspace stream. */
export type WorkspaceEvent = StreamAddress.CollectionsChange<typeof Workspaces.collections>;

/**
 * Stamp changes for one workspace. The tracker writes its own events, so the
 * head that the recipe read is their source offset.
 */
export const workspaceEvents = (
  workspaceId: string,
  sourceOffset: string,
  changes: Array.NonEmptyReadonlyArray<WorkspaceChange>,
) => DurableState.changes(Workspaces.at({ workspaceId }), { offset: sourceOffset }, changes);

/** SQLite when `ISSUE_TRACKER_DB` is set, otherwise a process-local memory store. */
export const StorageLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ databasePath }) => storage(databasePath)),
);

function storage(databasePath: Option.Option<string>): Layer.Layer<Workspaces, StorageFault> {
  return Option.match(databasePath, {
    onNone: () => MemoryStorage.layer(Streamsy.group(Workspaces)),
    onSome: (filename) => BunStorage.layer(Streamsy.group(Workspaces), { client: { filename } }),
  });
}
