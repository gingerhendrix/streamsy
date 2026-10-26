/* oxlint-disable effecttsgo/strict-effect-provide, unicorn/consistent-function-scoping -- Each test program is its own entry point, and the helpers stay beside the test that reads them. */
import { expect, it as baseIt } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Option, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { MemoryStorage, Streamsy } from "@streamsy/core";
import { AlarmSlot } from "../../src/cloudflare/alarm.ts";
import { withLinks } from "../../src/cloudflare/links.ts";
import { ObjectLink } from "../../src/cloudflare/object-link.ts";
import {
  ObjectStreams,
  objectRouter,
  objectTransportError,
  type ObjectFetch,
} from "../../src/cloudflare/object-streams.ts";
import { Placement, rule } from "../../src/cloudflare/placement.ts";
import { objectHandlers } from "../../src/alchemy.ts";

class Notes extends Streamsy.json<Notes>()("ObjectLinks/Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}

class Inbox extends Streamsy.wake<Inbox>()("ObjectLinks/Inbox", "inbox/:user", {
  params: { user: Schema.String },
}) {}

class Other extends Streamsy.json<Other>()("ObjectLinks/Other", "other/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}

const notes = Streamsy.group(Notes);

const placement = Placement.byRoute([
  rule({ stream: Notes, owner: ({ user }) => `user-${user}` }),
  rule({ stream: Inbox, owner: ({ user }) => `user-${user}` }),
]);

const link = ObjectLink.make({
  group: Streamsy.group(Notes, Inbox),
  prefix: "/__link",
  placement,
  access: "full",
});

interface Owner {
  readonly handler: (request: Request) => Promise<Response>;
  readonly dispose: () => Promise<void>;
}

/** The owner mount of `link` over its own memory storage, as one owner object serves it. */
const ownerObject = (): Owner => {
  const web = HttpRouter.toWebHandler(
    link.routes.pipe(Layer.provideMerge(MemoryStorage.layer(Streamsy.group(Notes, Inbox)))),
    { disableLogger: true },
  );

  return { handler: (request) => web.handler(request), dispose: () => web.dispose() };
};

/** One owner object for each name. `names` records each object call. */
const owners = () => {
  const names: Array<string> = [];
  const objects = new Map<string, Owner>();

  const fetch: ObjectFetch = (name, request) =>
    Effect.tryPromise({
      try: () => {
        names.push(name);
        const object = objects.get(name) ?? ownerObject();
        objects.set(name, object);

        return object.handler(request);
      },
      catch: (cause) => objectTransportError(request, cause),
    });

  const dispose = Effect.promise(() =>
    Promise.all(Array.from(objects.values(), (object) => object.dispose())),
  );

  return { names, fetch, dispose };
};

const forbidden: ObjectFetch = () => Effect.die(new Error("must not call an object"));

baseIt.effect(
  "ObjectStreams translates an Effect transport failure to an HTTP transport error",
  () =>
    Effect.gen(function* () {
      const failure = new Error("object unavailable");

      const error = yield* Effect.flatMap(Notes, (service) => service.create({ user: "ann" })).pipe(
        Effect.flip,
        Effect.provide(
          ObjectStreams.layer(notes, {
            objects: (_name, request) => Effect.fail(objectTransportError(request, failure)),
            placement,
            prefix: "/__link",
          }),
        ),
      );

      expect(error).toMatchObject({
        _tag: "TransportFault",
        reason: "request",
        cause: { _tag: "HttpClientError", reason: { _tag: "TransportError", cause: failure } },
      });
    }),
);

baseIt.effect("interrupting an object request aborts its signal and finalizes the transport", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<Request>();

    let finalized = false;

    const send: ObjectFetch = (_name, request) =>
      Effect.andThen(Deferred.succeed(entered, request), Effect.never).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            finalized = true;
          }),
        ),
      );

    yield* Effect.gen(function* () {
      const service = yield* Notes;
      const fiber = yield* service.create({ user: "ann" }).pipe(Effect.forkScoped);
      const request = yield* Deferred.await(entered);

      expect(request.signal.aborted).toBe(false);
      yield* Fiber.interrupt(fiber);
      expect(request.signal.aborted).toBe(true);
      expect(finalized).toBe(true);
    }).pipe(
      Effect.provide(ObjectStreams.layer(notes, { objects: send, placement, prefix: "/__link" })),
    );
  }),
);

baseIt.live("ObjectStreams sends each stream to the object that placement names", () =>
  Effect.gen(function* () {
    const owner = owners();
    yield* Effect.addFinalizer(() => owner.dispose);

    const written = yield* Effect.gen(function* () {
      const service = yield* Notes;
      yield* service.create({ user: "ann" });
      yield* service.create({ user: "bob" });
      yield* service.append({ user: "ann" }, ["a1"]);
      yield* service.append({ user: "bob" }, ["b1", "b2"]);

      return {
        ann: (yield* service.collect({ user: "ann" })).items,
        bob: (yield* service.collect({ user: "bob" })).items,
      };
    }).pipe(
      Effect.provide(
        ObjectStreams.layer(notes, { objects: owner.fetch, placement, prefix: "/__link" }),
      ),
    );

    expect(written).toEqual({ ann: ["a1"], bob: ["b1", "b2"] });
    expect(new Set(owner.names)).toEqual(new Set(["user-ann", "user-bob"]));
  }),
);

baseIt.live("a producer append through ObjectStreams settles a repeat as Duplicate", () =>
  Effect.gen(function* () {
    const owner = owners();
    yield* Effect.addFinalizer(() => owner.dispose);

    const tags = yield* Effect.gen(function* () {
      const service = yield* Notes;
      yield* service.create({ user: "ann" });
      const producer = { producerId: "p", producerEpoch: 0, producerSeq: 0 };
      const first = yield* service.append({ user: "ann" }, ["once"], { producer });
      const again = yield* service.append({ user: "ann" }, ["once"], { producer });
      const items = (yield* service.collect({ user: "ann" })).items;

      return { first: first._tag, again: again._tag, items };
    }).pipe(
      Effect.provide(
        ObjectStreams.layer(notes, { objects: owner.fetch, placement, prefix: "/__link" }),
      ),
    );

    expect(tags).toEqual({ first: "Appended", again: "Duplicate", items: ["once"] });
  }),
);

baseIt.live("objectRouter answers outside the prefix, the group or a valid placement here", () =>
  Effect.gen(function* () {
    const route = (placed: Placement) =>
      objectRouter(notes, { objects: forbidden, placement: placed, prefix: "/__link" });

    const status = (placed: Placement, path: string) =>
      Effect.map(
        route(placed)(new Request(`https://streamsy.internal${path}`)),
        (response) => response.status,
      );

    expect(yield* status(placement, "/streams/notes/ann")).toBe(404);
    expect(yield* status(placement, "/__link")).toBe(404);
    expect(yield* status(placement, "/__link/other/ann")).toBe(404);
    expect(yield* status(placement, "/__link/inbox/ann")).toBe(404);
    expect(
      yield* status(
        Placement.byKey(() => ""),
        "/__link/notes/ann",
      ),
    ).toBe(400);

    expect(
      yield* status(
        Placement.byKey(() => {
          throw new Error("bad owner");
        }),
        "/__link/notes/ann",
      ),
    ).toBe(500);
  }),
);

baseIt("ObjectLink.make refuses a prefix outside the reserved /__ space", () => {
  const prefixes: ReadonlyArray<`/__${string}`> = ["/__", "/__/x"];

  for (const prefix of prefixes)
    expect(() => ObjectLink.make({ group: notes, prefix, placement, access: "read" })).toThrow(
      RangeError,
    );

  expect(() =>
    // @ts-expect-error The prefix type refuses a path outside "/__".
    ObjectLink.make({ group: notes, prefix: "/link", placement, access: "read" }),
  ).toThrow(RangeError);
});

baseIt.live("link routes answer 405 for each method above the access, for wake streams too", () =>
  Effect.gen(function* () {
    const statuses = (access: "read" | "append" | "full") =>
      Effect.gen(function* () {
        const mount = ObjectLink.make({
          group: Streamsy.group(Notes, Inbox),
          prefix: "/__link",
          placement,
          access,
        });

        const web = HttpRouter.toWebHandler(
          mount.routes.pipe(Layer.provideMerge(MemoryStorage.layer(Streamsy.group(Notes, Inbox)))),
          { disableLogger: true },
        );

        yield* Effect.addFinalizer(() => Effect.promise(() => web.dispose()));

        const send = (method: string, path: string) =>
          Effect.promise(() =>
            web
              .handler(new Request(`https://streamsy.internal/__link/${path}`, { method }))
              .then((response) => response.status),
          );

        return {
          notesPost: yield* send("POST", "notes/ann"),
          inboxPost: yield* send("POST", "inbox/ann"),
          notesPut: yield* send("PUT", "notes/ann"),
          inboxPut: yield* send("PUT", "inbox/ann"),
        };
      });

    expect(yield* statuses("read")).toEqual({
      notesPost: 405,
      inboxPost: 405,
      notesPut: 405,
      inboxPut: 405,
    });

    const append = yield* statuses("append");
    expect([append.notesPut, append.inboxPut]).toEqual([405, 405]);
    expect([append.notesPost, append.inboxPost]).not.toContain(405);

    const full = yield* statuses("full");
    expect([full.notesPost, full.inboxPost, full.notesPut, full.inboxPut]).not.toContain(405);
  }),
);

baseIt.live("an own layer uses a linked stream at start, and builds the link once", () =>
  Effect.gen(function* () {
    const owner = owners();
    yield* Effect.addFinalizer(() => owner.dispose);
    let builds = 0;

    const counted = {
      streams: link.client(owner.fetch).streams,
      layer: Layer.unwrap(
        Effect.sync(() => {
          builds += 1;

          return link.client(owner.fetch).layer;
        }),
      ),
    };

    const startup = Layer.effectDiscard(
      Effect.flatMap(Notes, (service) => service.create({ user: "ann" })).pipe(Effect.orDie),
    );

    const items = yield* Effect.flatMap(Notes, (service) =>
      Effect.andThen(service.append({ user: "ann" }, ["late"]), service.collect({ user: "ann" })),
    ).pipe(Effect.provide(withLinks(startup, [counted])));

    expect(items.items).toEqual(["late"]);
    expect(builds).toBe(1);
    expect(owner.names).toContain("user-ann");
  }),
);

baseIt.live("an own layer that also provides a linked stream fails to start", () =>
  Effect.gen(function* () {
    const clashing = withLinks(MemoryStorage.layer(Streamsy.group(Notes, Other)), [
      {
        streams: link.group.streams,
        layer: ObjectStreams.layer(link.group, {
          objects: forbidden,
          placement,
          prefix: link.prefix,
        }),
      },
    ]);

    const exit = yield* Effect.exit(Effect.scoped(Layer.build(clashing)));

    expect(Exit.isFailure(exit)).toBe(true);

    const error = Exit.isFailure(exit)
      ? Option.getOrUndefined(Exit.findErrorOption(exit))
      : undefined;

    expect(error?._tag).toBe("StorageFault");
    expect(error?.message).toContain("ObjectLinks/Notes is linked to another object");
  }),
);

const slot = Layer.succeed(AlarmSlot, {
  current: Effect.succeedNone,
  arm: () => Effect.void,
  clear: Effect.void,
});

const get = (handlers: Effect.Success<ReturnType<typeof objectHandlers>>, path: string) =>
  handlers.fetch.pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(new Request(`https://streamsy.internal${path}`)),
    ),
    Effect.map((response) => response.status),
  );

baseIt.live("the Alchemy host refuses a clash and serves linked streams to its app", () =>
  Effect.gen(function* () {
    const owner = owners();
    yield* Effect.addFinalizer(() => owner.dispose);

    const probe = HttpRouter.add(
      "GET",
      "/probe",
      Effect.flatMap(Notes, (service) => service.create({ user: "ann" })).pipe(
        Effect.as(HttpServerResponse.text("ok")),
        Effect.orDie,
      ),
    );

    const clashing = yield* objectHandlers({
      app: probe,
      layer: Layer.merge(MemoryStorage.layer(Streamsy.group(Notes)), slot).pipe(
        Layer.provideMerge(MemoryStorage.layerProtocol()),
      ),
      links: [link.client(owner.fetch)],
    });

    expect(yield* get(clashing, "/probe")).toBe(503);

    const linked = yield* objectHandlers({
      app: probe,
      layer: Layer.merge(MemoryStorage.layer(Streamsy.group(Other)), slot).pipe(
        Layer.provideMerge(MemoryStorage.layerProtocol()),
      ),
      links: [link.client(owner.fetch)],
    });

    expect(yield* get(linked, "/probe")).toBe(200);
    expect(owner.names).toEqual(["user-ann"]);
  }),
);
