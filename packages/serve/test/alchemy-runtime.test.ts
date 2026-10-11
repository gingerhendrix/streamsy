/* oxlint-disable effecttsgo/any-unknown-in-error-context -- Exercises the public Wakeup.run boundary, whose app error type is unknown. */
import { BunStorage } from "@streamsy/storage/bun";
import { defineActionSink } from "@streamsy/serve/action";
import { OutboxStore } from "@streamsy/serve/action/outbox";
import {
  Wakeup as OutboxWakeup,
  actionSinkHandler,
  draftsFor,
} from "@streamsy/serve/action/runtime";
import { createSqlOutboxBacking, migrateOutbox } from "@streamsy/serve/action/sql";
import { SqlClient } from "effect/sql";
import {
  type StreamsReader,
  type StreamsWriter,
  routes as protocolRoutes,
} from "@streamsy/core/protocol";
import { expect, it as baseIt } from "@streamsy/effect-test";
import { Cause, Clock, Context, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect";
import { StorageFault, MemoryStorage, Streamsy } from "@streamsy/core";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { AlarmSlot } from "../src/cloudflare/alarm.ts";
import { TestClock } from "effect/testing";
import { Wakeup, Wakeups, makeWakeups } from "../src/cloudflare/wakeup.ts";
import { objectHandlers } from "../src/alchemy.ts";

baseIt.effect.each(["typed", "defect"])(
  "Alchemy retries %s acquisition failures and closes its scope",
  (failure) =>
    Effect.gen(function* () {
      let acquisitions = 0;
      let releases = 0;
      let alarms = 0;
      const protocol = MemoryStorage.layerProtocol();

      const layer = Layer.unwrap(
        Effect.gen(function* () {
          acquisitions += 1;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              releases += 1;
            }),
          );

          if (acquisitions === 1 && failure === "defect")
            return yield* Effect.die(new Error("fixture acquisition defect"));

          if (acquisitions === 1)
            return yield* new StorageFault({
              operation: "fixture.acquire",
              message: "first build fails",
              retryable: true,
            });

          return Layer.mergeAll(
            protocol,
            Layer.succeed(AlarmSlot, {
              current: Effect.succeedNone,
              arm: () => Effect.void,
              clear: Effect.sync(() => {
                alarms += 1;
              }),
            }),
          );
        }),
      );

      yield* Effect.gen(function* () {
        const handlers = yield* objectHandlers({
          app: protocolRoutes({ prefix: "/streams" }),
          layer,
        });

        expect(acquisitions).toBe(0);

        const fetch = handlers.fetch.pipe(
          Effect.provideService(
            HttpServerRequest.HttpServerRequest,
            HttpServerRequest.fromWeb(
              new Request("https://streams.test/streams/s", { method: "OPTIONS" }),
            ),
          ),
        );

        const failed = HttpServerResponse.toWeb(yield* fetch);
        expect(failed.status).toBe(503);
        expect(failed.headers.get("retry-after")).toBe("1");
        expect(failed.headers.get("x-content-type-options")).toBe("nosniff");
        expect(yield* Effect.promise(() => failed.text())).toBe("Storage unavailable");
        expect(releases).toBe(1);
        const responses = yield* Effect.all([fetch, fetch], { concurrency: "unbounded" });
        expect(responses.map((response) => response.status)).toEqual([204, 204]);

        const missing = yield* handlers.fetch.pipe(
          Effect.provideService(
            HttpServerRequest.HttpServerRequest,
            HttpServerRequest.fromWeb(new Request("https://streams.test/outside")),
          ),
        );

        expect(missing.status).toBe(404);

        yield* handlers.alarm();
        expect(acquisitions).toBe(2);
        expect(releases).toBe(1);
        expect(alarms).toBe(2);
      }).pipe(Effect.scoped);
      expect(releases).toBe(2);
    }),
);

const protocolStreams = Streamsy.group(Streamsy.Wildcard);

const request = (
  handlers: import("../src/alchemy.ts").ObjectHandlers,
  path: string,
  method = "GET",
  headers: Record<string, string> = {},
) =>
  handlers.fetch.pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(new Request(`https://streams.test${path}`, { method, headers })),
    ),
  );

baseIt.effect("multiple wakeups choose the earliest deadline and clear only together", () =>
  Effect.gen(function* () {
    let left = Option.some(20_000);
    let right = Option.some(10_000);
    let current = Option.none<number>();

    const context = yield* Layer.build(
      Layer.merge(
        MemoryStorage.layerProtocol(),
        Layer.succeed(AlarmSlot, {
          current: Effect.sync(() => current),
          arm: (at) =>
            Effect.sync(() => {
              current = Option.some(at);
            }),
          clear: Effect.sync(() => {
            current = Option.none();
          }),
        }),
      ),
    );

    const scheduler = yield* makeWakeups(context, [
      Wakeup.make({ name: "app/left", next: Effect.sync(() => left), run: Effect.void }),
      Wakeup.make({ name: "app/right", next: Effect.sync(() => right), run: Effect.void }),
    ]);

    expect(current).toEqual(Option.some(10_000));
    const service = Context.get(scheduler.context, Wakeups);
    right = Option.none();
    yield* service.reconcile;
    expect(current).toEqual(Option.some(10_000)); // harmless early alarm
    left = Option.none();
    yield* service.reconcile;
    expect(current).toEqual(Option.none());
  }),
);

baseIt.effect.each(["failure", "still due"])("%s backs off and caps", (mode) =>
  Effect.gen(function* () {
    let due = Option.some(0);
    let fail = mode === "failure";
    let current = Option.none<number>();

    const context = yield* Layer.build(
      Layer.merge(
        MemoryStorage.layerProtocol(),
        Layer.succeed(AlarmSlot, {
          current: Effect.sync(() => current),
          arm: (at) =>
            Effect.sync(() => {
              current = Option.some(at);
            }),
          clear: Effect.sync(() => {
            current = Option.none();
          }),
        }),
      ),
    );

    const scheduler = yield* makeWakeups(context, [
      Wakeup.make({
        name: "app/retry",
        next: Effect.sync(() => due),
        run: Effect.suspend(() => (fail ? Effect.fail("down") : Effect.void)),
        retry: { initial: "1 second", max: "2 seconds" },
      }),
    ]);

    const service = Context.get(scheduler.context, Wakeups);

    for (const [now, target, failures] of [
      [0, 1000, 1],
      [1000, 3000, 2],
      [3000, 5000, 3],
    ]) {
      yield* TestClock.setTime(now!);
      yield* scheduler.alarm;
      expect(current).toEqual(Option.some(target!));
      expect((yield* service.status)[1]?.failures).toBe(failures);
    }
  }),
);

baseIt.effect.each(["failure", "still due"])(
  "a %s run resets failures after advancing its deadline",
  (mode) =>
    Effect.gen(function* () {
      let due = Option.some(0);
      let progress = false;

      const context = yield* Layer.build(
        Layer.merge(
          MemoryStorage.layerProtocol(),
          Layer.succeed(AlarmSlot, {
            current: Effect.succeedNone,
            arm: () => Effect.void,
            clear: Effect.void,
          }),
        ),
      );

      const scheduler = yield* makeWakeups(context, [
        Wakeup.make({
          name: "app/progress",
          next: Effect.sync(() => due),
          run: Effect.suspend(() => {
            if (!progress && mode === "failure") return Effect.fail("down");

            return Effect.sync(() => {
              if (progress) due = Option.some(10_000);
            });
          }),
        }),
      ]);

      yield* scheduler.alarm;
      progress = true;
      yield* TestClock.setTime(1000);
      yield* scheduler.alarm;
      expect((yield* Context.get(scheduler.context, Wakeups).status)[1]?.failures).toBe(0);
    }),
);

baseIt.effect("duplicate names, including expiry, defect at acquire", () =>
  Effect.gen(function* () {
    const context = yield* Layer.build(
      Layer.merge(
        MemoryStorage.layerProtocol(),
        Layer.succeed(AlarmSlot, {
          current: Effect.succeedNone,
          arm: () => Effect.void,
          clear: Effect.void,
        }),
      ),
    );

    for (const names of [["app/same", "app/same"], ["streamsy/expiry"]]) {
      const exit = yield* Effect.exit(
        makeWakeups(
          context,
          names.map((name) => Wakeup.make({ name, next: Effect.succeedNone, run: Effect.void })),
        ),
      );

      expect(Exit.isFailure(exit)).toBe(true);

      if (Exit.isFailure(exit)) expect(Cause.hasDies(exit.cause)).toBe(true);
    }
  }),
);

baseIt.effect(
  "GET touch reconciles, an ordinary GET does not, and acquire restores a committed deadline",
  () =>
    Effect.gen(function* () {
      let due = Option.some(10_000);
      let current = Option.none<number>();
      let arms = 0;

      const app = Layer.merge(
        HttpRouter.add("GET", "/read", HttpServerResponse.text("read")),
        HttpRouter.add(
          "GET",
          "/touch",
          Effect.gen(function* () {
            yield* Wakeups.use((service) => service.touch);

            return HttpServerResponse.text("touched");
          }),
        ),
      );

      const handlers = yield* objectHandlers({
        app,
        layer: Layer.merge(
          MemoryStorage.layerProtocol(),
          Layer.succeed(AlarmSlot, {
            current: Effect.sync(() => current),
            arm: (at) =>
              Effect.sync(() => {
                current = Option.some(at);
                arms++;
              }),
            clear: Effect.void,
          }),
        ),
        wakeups: [
          Wakeup.make({ name: "app/deadline", next: Effect.sync(() => due), run: Effect.void }),
        ],
      });

      yield* request(handlers, "/read");
      expect(current).toEqual(Option.some(10_000));
      expect(arms).toBe(1);
      due = Option.some(5000);
      yield* request(handlers, "/read");
      expect(arms).toBe(1);
      yield* request(handlers, "/touch");
      expect(current).toEqual(Option.some(5000));
      expect(arms).toBe(2);
    }),
);

baseIt.effect("a failing app wakeup leaves expiry running and rearmed", () =>
  Effect.gen(function* () {
    let current = Option.none<number>();

    const handlers = yield* objectHandlers<StreamsReader | StreamsWriter>({
      app: protocolRoutes({ prefix: "/streams" }),
      layer: Layer.merge(
        MemoryStorage.layerProtocol(),
        Layer.succeed(AlarmSlot, {
          current: Effect.sync(() => current),
          arm: (at) =>
            Effect.sync(() => {
              current = Option.some(at);
            }),
          clear: Effect.sync(() => {
            current = Option.none();
          }),
        }),
      ),
      wakeups: [
        Wakeup.make({ name: "app/fails", next: Effect.succeedSome(0), run: Effect.fail("down") }),
      ],
    });

    yield* request(handlers, "/streams/early", "PUT", { "stream-ttl": "1" });
    yield* request(handlers, "/streams/later", "PUT", { "stream-ttl": "10" });
    yield* TestClock.setTime(1000);
    yield* handlers.alarm();
    expect((yield* request(handlers, "/streams/early", "HEAD")).status).toBe(404);
    expect((yield* request(handlers, "/streams/later", "HEAD")).status).toBe(200);
    expect(current).toEqual(Option.some(2000));
    yield* TestClock.setTime(10_000);
    yield* handlers.alarm();
    expect((yield* request(handlers, "/streams/later", "HEAD")).status).toBe(404);
  }),
);

baseIt.effect("request and alarm runs of a wakeup share one permit", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let active = 0;
    let maximum = 0;
    let runs = 0;

    const wakeup = Wakeup.make({
      name: "app/serialized",
      next: Effect.succeedSome(0),
      run: Effect.gen(function* () {
        active++;
        maximum = Math.max(maximum, active);
        runs++;
        yield* Deferred.succeed(started, undefined);
        yield* Deferred.await(release);
        active--;
      }),
    });

    const context = yield* Layer.build(
      Layer.merge(
        MemoryStorage.layerProtocol(),
        Layer.succeed(AlarmSlot, {
          current: Effect.succeedNone,
          arm: () => Effect.void,
          clear: Effect.void,
        }),
      ),
    );

    const scheduler = yield* makeWakeups(context, [wakeup]);
    const inRequest = yield* Effect.forkChild(wakeup.run.pipe(Effect.provide(scheduler.context)));
    yield* Deferred.await(started);
    const inAlarm = yield* Effect.forkChild(scheduler.alarm);
    yield* Effect.yieldNow;
    expect(runs).toBe(1);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(inRequest);
    yield* Fiber.join(inAlarm);
    expect(runs).toBe(2);
    expect(maximum).toBe(1);
  }),
);

baseIt.effect("POST enqueue arms the alarm and its turn delivers the SQLite outbox", () =>
  Effect.gen(function* () {
    let scheduled = Option.none<number>();

    const notices = defineActionSink<{ readonly id: string }, { readonly key: "id" }>({
      name: "folder.notice",
      from: { key: "id" },
      handler: { name: "index", version: 1 },
      payload: { encode: JSON.stringify, decode: () => ({ id: "docs/version-1" }) },
      idempotencyKey: (notice) => notice.id,
      partitionBy: () => "docs",
      delivery: { maxAttempts: 3, initialBackoffMs: 1000, maxBackoffMs: 10_000, backoffFactor: 2 },
    });

    const outbox = Layer.effect(
      OutboxStore,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* migrateOutbox(sql);

        return OutboxStore.of(createSqlOutboxBacking(sql));
      }),
    );

    const layer = outbox.pipe(
      Layer.provideMerge(BunStorage.layer(protocolStreams, { client: { filename: ":memory:" } })),
    );

    const context = yield* Layer.build(layer);
    const store = Context.get(context, OutboxStore);
    const delivered: string[] = [];

    const handler = actionSinkHandler(notices, (delivery) =>
      Effect.sync(() => {
        delivered.push(delivery.idempotencyKey);
      }),
    );

    const handlers = yield* objectHandlers({
      app: HttpRouter.add(
        "POST",
        "/enqueue",
        Effect.gen(function* () {
          const queue = yield* OutboxStore;
          yield* queue
            .enqueue(draftsFor(notices, [{ id: "docs/version-1" }], yield* Clock.currentTimeMillis))
            .pipe(Effect.orDie);

          return HttpServerResponse.text("queued");
        }),
      ),
      layer: Layer.merge(
        Layer.succeedContext(context),
        Layer.succeed(AlarmSlot, {
          current: Effect.sync(() => scheduled),
          arm: (at) =>
            Effect.sync(() => {
              scheduled = Option.some(at);
            }),
          clear: Effect.sync(() => {
            scheduled = Option.none();
          }),
        }),
      ),
      wakeups: [OutboxWakeup.outbox(notices, handler, { ordered: true })],
    });

    expect((yield* request(handlers, "/enqueue", "POST")).status).toBe(200);
    expect(scheduled).toEqual(Option.some(1));
    expect((yield* store.list(notices.name, undefined))[0]?.state).toBe("pending");
    yield* TestClock.setTime(1);
    yield* handlers.alarm();
    expect(delivered).toEqual(["docs/version-1"]);
    expect((yield* store.list(notices.name, undefined))[0]?.state).toBe("delivered");
    expect(scheduled).toEqual(Option.none());
  }),
);
