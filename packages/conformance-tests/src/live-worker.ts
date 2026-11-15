/* oxlint-disable effecttsgo/async-function -- This test entry owns the real workerd boundary. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  ExportedHandler,
  WebSocket,
} from "@cloudflare/workers-types";
import { Effect, Layer, Option } from "effect";
import { HttpRouter, HttpServerRequest } from "effect/http";
import { Http, Storage, StreamId, Streamsy } from "@streamsy/core";
import {
  Placement,
  StreamsyObject,
  Wakeup,
  router,
  type LiveConfiguration,
} from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";

/**
 * The workerd entry for `test/workerd.websocket.test.ts`. It is not the
 * conformance worker and is never deployed. `/__test/*` routes report and
 * change object state for the tests.
 */
interface Env {
  readonly LIVE: DurableObjectNamespace;
  readonly SMALL: DurableObjectNamespace;
  readonly PLAIN: DurableObjectNamespace;
}

const streams = Streamsy.group(Streamsy.Wildcard);

const encoder = new TextEncoder();

/** Per isolate. Workerd builds a new object instance after hibernation. */
const counters = { constructions: 0, messages: 0, reads: 0 };

/** Counts every `GET` that reaches the app, including the host's internal pushes. */
const countReads = HttpRouter.middleware(
  (handler) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;

      if (request.method === "GET") counters.reads += 1;

      return yield* handler;
    }),
  { global: true },
);

/** While `alarm/trigger` exists, the alarm appends to `alarm/target` and removes the trigger. */
const alarmAppend = Wakeup.make<Storage | Streamsy.Wildcard>({
  name: "test/alarm-append",
  next: Storage.use((storage) => storage.record(StreamId.make("alarm/trigger"))).pipe(
    Effect.map(Option.map(() => 0)),
  ),
  run: Effect.gen(function* () {
    const wildcard = yield* Streamsy.Wildcard;
    yield* wildcard.wire.append(StreamId.make("alarm/target"), {
      data: encoder.encode("from-alarm"),
      contentType: "text/plain",
    });
    yield* wildcard.wire.remove(StreamId.make("alarm/trigger"));
  }),
});

const objectClass = (prefix: `/${string}`, live: LiveConfiguration | undefined) =>
  class extends StreamsyObject.make<Env, Streamsy.Services<typeof streams>>({
    app: Layer.merge(Http.routes(streams, { prefix, access: "full" }), countReads),
    layer: (state) =>
      DurableObjectStorage.layer(streams, {
        client: { storage: state.storage },
        longPollTimeoutMs: 1_500,
      }),
    wakeups: [alarmAppend],
    live,
  }) {
    // The instance id tells a test that workerd built a new instance after hibernation.
    // oxlint-disable-next-line effecttsgo/crypto-random-uuid -- A plain class field outside Effect.
    readonly #instance = crypto.randomUUID();
    readonly #state: DurableObjectState;

    constructor(state: DurableObjectState, env: Env) {
      super(state, env);
      this.#state = state;
      counters.constructions += 1;
    }

    override fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);

      if (url.pathname === "/__test/state")
        return Promise.resolve(Response.json({ instance: this.#instance, ...counters }));

      if (url.pathname === "/__test/reset-reads") {
        counters.reads = 0;

        return Promise.resolve(Response.json({ ok: true }));
      }

      if (url.pathname === "/__test/attachments") {
        const version = Number(url.searchParams.get("v"));

        for (const ws of this.#state.getWebSockets("streamsy:live"))
          ws.serializeAttachment({ v: version });

        return Promise.resolve(Response.json({ ok: true }));
      }

      return super.fetch(request);
    }

    override webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
      counters.messages += 1;

      return super.webSocketMessage(ws, message);
    }
  };

/** Default options, mounted at `/`. */
export class LiveObject extends objectClass("/", { websocket: true }) {}

/** Small caps, mounted at `/small`. */
export class SmallObject extends objectClass("/small", {
  websocket: { prefix: "/small", maxSockets: 2, maxCatchUpBytes: 64, maxPushBytes: 64 },
}) {}

/** The transport left off, mounted at `/plain`. */
export class PlainObject extends objectClass("/plain", undefined) {}

/** Streams that share a first path segment share an object, so a wakeup can reach both. */
const byGroup = Placement.byKey((path) => path.split("/")[0] ?? path);

const live = router<Env>({ placement: byGroup, namespace: (env) => env.LIVE });

const small = router<Env>({
  prefix: "/small",
  placement: byGroup,
  namespace: (env) => env.SMALL,
});

const plain = router<Env>({
  prefix: "/plain",
  placement: byGroup,
  namespace: (env) => env.PLAIN,
});

export default {
  fetch(request, env, context) {
    const url = new URL(request.url);

    // `/__test/<route>?stream=<path>` reaches the object that holds `<path>`.
    if (url.pathname.startsWith("/__test/")) {
      const stream = url.searchParams.get("stream") ?? "";
      const namespace = stream.startsWith("small/") ? env.SMALL : env.LIVE;
      const group = stream.replace(/^small\//, "").split("/")[0] ?? "";

      return namespace.get(namespace.idFromName(group)).fetch(request.url, { method: "POST" });
    }

    const target = url.pathname.startsWith("/small/")
      ? small
      : url.pathname.startsWith("/plain/")
        ? plain
        : live;

    return target.fetch?.(request, env, context) ?? new Response(null, { status: 500 });
  },
} satisfies ExportedHandler<Env>;
