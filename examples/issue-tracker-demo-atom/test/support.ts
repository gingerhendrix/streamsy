import { Array, Deferred, Effect, Layer, Option, Queue, Stream } from "effect";
import {
  HttpClient,
  HttpClientError,
  type HttpClientRequest,
  HttpClientResponse,
} from "effect/http";
import { Atom, AtomRegistry } from "effect/reactivity";
import type { JsonValue } from "@streamsy/issue-tracker-demo-contract/state-schema";
import type {
  Comment,
  Issue,
  Project,
  StateEvent,
} from "@streamsy/issue-tracker-demo-contract/types";
import type { Stamp } from "../src/domain/intent.ts";
import { ReadEvent } from "../src/domain/workspace.ts";
import { atomRuntime, clientLayer, WorkspaceReader } from "../src/runtime.ts";
import { workspaceIdAtom } from "../src/state.ts";

export const project: Project = {
  id: "proj_demo",
  name: "Demo",
  description: "",
  createdAt: "2026-11-05T10:00:00.000Z",
};

export const issue = (id: string, fields: Partial<Issue> = {}): Issue => ({
  id,
  projectId: project.id,
  title: `Issue ${id}`,
  status: "open",
  createdAt: "2026-11-05T10:00:00.000Z",
  updatedAt: "2026-11-05T10:00:00.000Z",
  ...fields,
});

export const comment = (id: string, issueId: string, createdAt: string): Comment => ({
  id,
  issueId,
  author: "anonymous",
  body: `Comment ${id}`,
  createdAt,
});

const headers = (txid?: string) =>
  txid === undefined ? { operation: "upsert" as const } : { operation: "upsert" as const, txid };

/** An upsert change as the State stream serves it. */
export const upsertProject = (value: Project, txid?: string): StateEvent => ({
  type: "project",
  key: value.id,
  value,
  headers: headers(txid),
});

export const upsertIssue = (value: Issue, txid?: string): StateEvent => ({
  type: "issue",
  key: value.id,
  value,
  headers: headers(txid),
});

export const upsertComment = (value: Comment, txid?: string): StateEvent => ({
  type: "comment",
  key: value.id,
  value,
  headers: headers(txid),
});

export const stamp = (txid: string): Stamp => ({
  entropy: "abcdef12-0000-4000-8000-000000000000",
  txid,
  at: "2026-11-05T12:00:00.000Z",
});

/** The JSON body of a write, as the client sent it. */
export type SentBody = Partial<Issue> & { readonly txid?: string };

/** The row with this id. A missing row fails the assertion that reads it. */
export const byId = <A extends { readonly id: string }>(rows: ReadonlyArray<A>, id: string) =>
  Option.getOrUndefined(Array.findFirst(rows, (row) => row.id === id));

export const ids = (rows: ReadonlyArray<{ readonly id: string }>) =>
  Array.map(rows, ({ id }) => id);

/** One request that the scripted server holds until the test answers it. */
export interface HeldRequest {
  readonly method: string;
  readonly path: string;
  readonly body: SentBody;
  readonly wasAborted: () => boolean;
  readonly answer: (status: number, body: JsonValue) => void;
  readonly dropConnection: () => void;
}

/** Let the registry and the fibers run. */
export const settle = Effect.sleep("5 millis");

const decodeBody = (request: HttpClientRequest.HttpClientRequest): SentBody =>
  request.body._tag === "Uint8Array" ? JSON.parse(new TextDecoder().decode(request.body.body)) : {};

/**
 * A registry with a scripted server and stream. The HTTP client is scripted
 * below the typed client, so a refusal goes through the contract's decoding.
 */
export const makeHarness = () => {
  const requests: Array<HeldRequest> = [];
  const events = Effect.runSync(Queue.unbounded<ReadEvent>());

  const httpClient = HttpClient.make((request, url, signal) =>
    Effect.gen(function* () {
      const held = yield* Deferred.make<Response, "dropped">();

      requests.push({
        method: request.method,
        path: url.pathname,
        body: decodeBody(request),
        wasAborted: () => signal.aborted,
        answer: (status, body) =>
          Deferred.doneUnsafe(
            held,
            Effect.succeed(
              new Response(JSON.stringify(body), {
                status,
                headers: { "content-type": "application/json" },
              }),
            ),
          ),
        dropConnection: () => Deferred.doneUnsafe(held, Effect.fail("dropped")),
      });

      const response = yield* Deferred.await(held);

      return HttpClientResponse.fromWeb(request, response);
    }).pipe(
      Effect.mapError(
        () =>
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({ request }),
          }),
      ),
    ),
  );

  const layer = Layer.mergeAll(
    clientLayer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, httpClient))),
    Layer.succeed(WorkspaceReader, { read: () => Stream.fromQueue(events) }),
  );

  const registry = AtomRegistry.make({
    initialValues: [
      Atom.initialValue(atomRuntime.layer, layer),
      Atom.initialValue(workspaceIdAtom, "main"),
    ],
  });

  /** Deliver one batch from the stream. */
  const receive = (...changes: ReadonlyArray<StateEvent>) =>
    Effect.andThen(Queue.offer(events, ReadEvent.Batch({ changes, isUpToDate: true })), settle);

  /** End the read with a failure. */
  const fail = (error: string) =>
    Effect.andThen(Queue.offer(events, ReadEvent.Failed({ error })), settle);

  return { registry, requests, receive, fail };
};
