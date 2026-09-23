import { State, StreamRef, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Effect, type Layer } from "effect";
import { CommentCodec, IssueCodec, ProjectCodec } from "../shared/state-schema.ts";
import { databasePath, workspaceStreamId } from "./config.ts";

/** One multi-collection ref for reading and writing a workspace stream. */
export const workspaceEvents = (workspaceId: string) =>
  StreamRef.state(workspaceStreamId(workspaceId), {
    collections: {
      project: { schema: ProjectCodec, key: "id" },
      issue: { schema: IssueCodec, key: "id" },
      comment: { schema: CommentCodec, key: "id" },
    },
  });

type WorkspaceCollections = ReturnType<typeof workspaceEvents>["collections"];
export type WorkspaceEvent = StreamRef.CollectionsChange<WorkspaceCollections>;
export type WorkspaceChange = State.Change<WorkspaceCollections>;

export function appendWorkspaceEvent(
  workspaceId: string,
  change: WorkspaceChange,
  sourceOffset: string,
  expectedOffset?: string,
) {
  const events = State.changes(workspaceEvents(workspaceId), { offset: sourceOffset }, [change]);
  const options = expectedOffset === undefined ? {} : { expectedOffset };
  return Streams.append(workspaceEvents(workspaceId), events, options);
}

/** Minimal runtime surface used by the Promise-native Bun route handlers. */
export interface DemoRuntime {
  runPromise<A, E>(effect: Effect.Effect<A, E, StreamsReader | StreamsWriter>): Promise<A>;
}

export const applicationLayer: Layer.Layer<
  StreamsReader | StreamsWriter,
  import("@streamsy/core").StorageFault
> =
  databasePath === undefined
    ? Streams.layerMemory()
    : BunStorage.layerProtocol({ client: { filename: databasePath } });
