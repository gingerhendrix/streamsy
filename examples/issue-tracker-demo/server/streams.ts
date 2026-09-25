import { DurableState, Memory, StreamAddress, Streamsy, type StorageFault } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Effect, Layer, Option, Schema } from "effect";
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
export const workspaceEvents = (workspaceId: string) => Workspaces.at({ workspaceId });

type WorkspaceCollections = ReturnType<typeof workspaceEvents>["collections"];
export type WorkspaceEvent = StreamAddress.CollectionsChange<WorkspaceCollections>;
export type WorkspaceChange = DurableState.Change<WorkspaceCollections>;

export function appendWorkspaceEvent(
  workspaceId: string,
  change: WorkspaceChange,
  sourceOffset: string,
  expectedOffset?: string,
) {
  const events = DurableState.changes(workspaceEvents(workspaceId), { offset: sourceOffset }, [
    change,
  ]);
  const options = expectedOffset === undefined ? {} : { expectedOffset };
  return Effect.flatMap(Workspaces, (workspaces) =>
    workspaces.append({ workspaceId }, events, options),
  );
}

/** SQLite when `ISSUE_TRACKER_DB` is set, otherwise a process-local memory store. */
export const StorageLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ databasePath }) => storage(databasePath)),
);

function storage(databasePath: Option.Option<string>): Layer.Layer<Workspaces, StorageFault> {
  return Option.match(databasePath, {
    onNone: () => Memory.layer({}, Streamsy.group(Workspaces)),
    onSome: (filename) => BunStorage.layer({ client: { filename } }, Streamsy.group(Workspaces)),
  });
}
