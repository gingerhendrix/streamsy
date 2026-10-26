/* oxlint-disable effecttsgo/async-function -- The native Worker and Durable Object boundaries are promise based. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  Request as WorkerRequest,
  Response as WorkerResponse,
} from "@cloudflare/workers-types";
import { Context, Effect, Layer } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { Http, Streamsy } from "@streamsy/core";
import { CheckpointStore, ProjectionWakeup } from "@streamsy/projection";
import { Serve } from "@streamsy/serve";
import { ObjectLink, Placement, rule, StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { Files, filesWake, UserWake } from "./wake-streams.ts";

/**
 * One folder object and one user object. A folder write reaches the user's wake
 * stream through `Projection.wake`, run by a `ProjectionWakeup`. The user
 * object creates its wake stream when the user is set up. The producer appends
 * through an `ObjectLink` over the user object stub, to an internal mount that
 * the Worker does not route.
 */

interface Env {
  readonly FOLDERS: DurableObjectNamespace;
  readonly USERS: DurableObjectNamespace;
  readonly CLASH: DurableObjectNamespace;
}

/** Test-only alarm visibility for a folder object. */
class FolderAlarm extends Context.Service<
  FolderAlarm,
  {
    readonly alarm: Effect.Effect<number | null>;
  }
>()("WakePair/FolderAlarm") {}

const folderAlarm = (state: DurableObjectState) =>
  Layer.succeed(
    FolderAlarm,
    FolderAlarm.of({
      alarm: Effect.promise(() => state.storage.getAlarm()),
    }),
  );

/** Folder objects append wake rows to the user object that owns each wake stream. */
const wakeLink = ObjectLink.make({
  group: Streamsy.group(UserWake),
  prefix: "/__wake",
  placement: Placement.byRoute([rule({ stream: UserWake, owner: ({ user }) => user })]),
  // The user object creates each wake stream; folder objects only append.
  access: "append",
});

const folderLayer = (state: DurableObjectState) =>
  Layer.mergeAll(CheckpointStore.layer, folderAlarm(state)).pipe(
    Layer.provideMerge(
      DurableObjectStorage.layer(Streamsy.group(Files), { client: { storage: state.storage } }),
    ),
  );

const FolderInternal = Layer.mergeAll(
  // A write that arms no alarm: the host reconciles only after PUT, POST and
  // DELETE. With a restart after it, this stands in for a crash between the
  // source commit and the alarm.
  HttpRouter.add(
    "GET",
    "/__quiet-append/:user/:folderId/:item",
    Effect.gen(function* () {
      const route = yield* HttpRouter.params;
      const folderId = route["folderId"] ?? "";

      const appended = yield* (yield* Files).append({ user: route["user"] ?? "", folderId }, [
        route["item"] ?? "",
      ]);

      return HttpServerResponse.text(appended.offset);
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/__folder/:folderId/alarm",
    Effect.map(
      Effect.flatMap(FolderAlarm, (meta) => meta.alarm),
      (alarm) => HttpServerResponse.text(alarm === null ? "none" : "armed"),
    ),
  ),
);

export class FolderObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof folderLayer>>,
  UserWake
>({
  app: Layer.mergeAll(
    Http.routes(Streamsy.group(Files), { prefix: "/streams", access: "full" }),
    FolderInternal,
  ),
  layer: folderLayer,
  links: (env) => [wakeLink.client(env.USERS)],
  wakeups: [ProjectionWakeup.attach(filesWake)],
}) {}

const userLayer = (state: DurableObjectState) =>
  DurableObjectStorage.layer(Streamsy.group(UserWake), { client: { storage: state.storage } });

/** Test-only user setup: the owner creates the wake stream before any folder writes. */
const createWake = HttpRouter.add(
  "GET",
  "/__create-wake/:user",
  Effect.gen(function* () {
    const route = yield* HttpRouter.params;
    yield* (yield* UserWake).create({ user: route["user"] ?? "" });

    return HttpServerResponse.empty({ status: 201 });
  }).pipe(Effect.orDie),
);

export class UserObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof userLayer>>
>({
  app: Layer.mergeAll(
    // Only object stubs reach this mount: the Worker below does not route `/__wake`.
    wakeLink.routes,
    Serve.mount(UserWake, "/users/:user/wake"),
    createWake,
  ),
  layer: userLayer,
}) {}

/**
 * A misconfigured caller: its storage group also holds the linked `UserWake`.
 * Its wake rows would go to a local stream that no client reads, so the host
 * refuses to start it.
 */
const clashLayer = (state: DurableObjectState) =>
  DurableObjectStorage.layer(Streamsy.group(Files, UserWake), {
    client: { storage: state.storage },
  });

export class ClashObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof clashLayer>>,
  UserWake
>({
  app: Http.routes(Streamsy.group(Files), { prefix: "/clash", access: "full" }),
  layer: clashLayer,
  links: (env) => [wakeLink.client(env.USERS)],
}) {}

export default {
  async fetch(request: WorkerRequest, env: Env): Promise<Response | WorkerResponse> {
    const path = new URL(request.url).pathname;

    const folder =
      /^\/streams\/users\/[^/]+\/folders\/([^/]+)/.exec(path) ??
      /^\/__quiet-append\/[^/]+\/([^/]+)/.exec(path) ??
      /^\/__folder\/([^/]+)/.exec(path);

    const user = /^\/users\/([^/]+)\/wake$/.exec(path) ?? /^\/__create-wake\/([^/]+)$/.exec(path);

    if (path.startsWith("/clash/"))
      return env.CLASH.get(env.CLASH.idFromName("clash")).fetch(request);

    const namespace =
      folder?.[1] !== undefined
        ? { ns: env.FOLDERS, name: folder[1] }
        : user?.[1] !== undefined
          ? { ns: env.USERS, name: user[1] }
          : undefined;

    if (namespace === undefined) return new Response(null, { status: 404 });

    return namespace.ns
      .get(namespace.ns.idFromName(decodeURIComponent(namespace.name)))
      .fetch(request);
  },
};
