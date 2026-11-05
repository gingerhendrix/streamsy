import { Context, Layer, type Stream } from "effect";
import { FetchHttpClient, type HttpClient } from "effect/http";
import { HttpApiClient } from "effect/http-api";
import { Atom } from "effect/reactivity";
import { IssueTrackerApi } from "@streamsy/issue-tracker-demo-contract/api";
import type { ReadEvent } from "./domain/workspace.ts";
import { streamReader } from "./stream.ts";

/** The typed client for `IssueTrackerApi`, with one method for each endpoint. */
export class IssueTrackerClient extends Context.Service<
  IssueTrackerClient,
  HttpApiClient.ForApi<typeof IssueTrackerApi>
>()("IssueTrackerDemoAtom/IssueTrackerClient") {}

/** The client over any `HttpClient`. The tests give it a scripted server. */
export const clientLayer: Layer.Layer<IssueTrackerClient, never, HttpClient.HttpClient> =
  Layer.effect(IssueTrackerClient, HttpApiClient.make(IssueTrackerApi));

/** Reads one workspace's State stream. */
export class WorkspaceReader extends Context.Service<
  WorkspaceReader,
  { readonly read: (workspaceId: string) => Stream.Stream<ReadEvent> }
>()("IssueTrackerDemoAtom/WorkspaceReader") {}

export const liveClient = clientLayer.pipe(Layer.provide(FetchHttpClient.layer));

const live = Layer.mergeAll(liveClient, Layer.succeed(WorkspaceReader, { read: streamReader }));

/**
 * The services that atoms run with. A test replaces `atomRuntime.layer` with
 * `Atom.initialValue`.
 */
export const atomRuntime = Atom.runtime(live);
