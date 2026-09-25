import { DurableState, StreamAddress, Streamsy, Memory } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Effect, Schema, type Layer } from "effect";
import { CommentCodec, IssueCodec, ProjectCodec } from "../shared/state-schema.ts";
import { databasePath } from "./config.ts";

/** One stream owns every workspace's multi-collection stream. */
export class Workspaces extends Streamsy.state<Workspaces>()(
  "IssueTrackerDemo/Workspaces",
  "workspace/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    collections: {
      project: { schema: ProjectCodec, key: "id" },
      issue: { schema: IssueCodec, key: "id" },
      comment: { schema: CommentCodec, key: "id" },
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

/** Minimal runtime surface used by the Promise-native Bun route handlers. */
export interface DemoRuntime {
  runPromise<A, E>(effect: Effect.Effect<A, E, Workspaces>): Promise<A>;
}

export const applicationLayer: Layer.Layer<Workspaces, import("@streamsy/core").StorageFault> =
  databasePath === undefined
    ? Memory.layer({}, Streamsy.group(Workspaces))
    : BunStorage.layer({ client: { filename: databasePath } }, Streamsy.group(Workspaces));
