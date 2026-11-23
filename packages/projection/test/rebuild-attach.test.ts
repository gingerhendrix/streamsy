import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Context, Deferred, Effect, Exit, Layer, Option, Ref, Schema, Scope } from "effect";
import { MemoryStorage, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  ProjectionWakeup,
} from "@streamsy/projection";
import { CheckpointRecord, recordKey, type Loaded } from "@streamsy/projection/checkpoint";
import { canonicalParams, encodeKey } from "../src/unit.ts";
import { AlarmSlot } from "../../serve/src/cloudflare/alarm.ts";
import { makeWakeups } from "../../serve/src/cloudflare/wakeup.ts";
import { eventually } from "./support/eventually.ts";
import { withFamilies } from "./support/families.ts";
import {
  Accepted,
  Facts,
  Numbers,
  accepted,
  declaration,
  output,
} from "./support/generation-outputs.ts";
import { layerSqlite } from "./support/sqlite.ts";

const host = withFamilies(layerSqlite(1), Facts, Numbers, Accepted);

const numbers = (
  id: string,
  generation: number,
  commit: typeof Commit.transaction | typeof Commit.producer = Commit.transaction,
) =>
  Projection.emit({
    ...declaration,
    id,
    generation,
    outputs: { numbers: output },
    accepted,
    commit,
    emit: ({ item }) => ({ numbers: [item.n] }),
  });

const seed = (workspace: string) =>
  Effect.gen(function* () {
    const facts = yield* Facts;
    yield* facts.create({ workspace });
    yield* facts.append({ workspace }, [{ id: "a", n: 3 }]);
    yield* facts.append({ workspace }, [{ id: "b", n: 5 }]);
  });

const items = (workspace: string, rev: number) =>
  Effect.gen(function* () {
    const result = yield* (yield* Numbers).collect({ workspace, rev });

    return result.items;
  }).pipe(Effect.catchTag("StreamNotFound", () => Effect.succeed([])));

layer(host)((it) => {
  it.effect(
    "replays two inactive bindings under the same id and returns before the handler finishes",
    () =>
      Effect.gen(function* () {
        const old = numbers("in-place", 1, Commit.producer);
        const workspaces = ["west", "east"];

        for (const workspace of workspaces) {
          yield* seed(workspace);
          yield* Projection.run(old.at({ workspace }));
        }

        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        const next = Projection.emit({
          ...declaration,
          id: old.id,
          generation: 2,
          outputs: { numbers: output },
          accepted,
          commit: Commit.producer,
          emit: ({ item }) =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as({ numbers: [item.n * 10] }),
            ),
        });

        yield* Layer.build(Projection.attach(next, { durable: true }));
        yield* Deferred.await(entered);

        const target = yield* Numbers;
        const wake = yield* Accepted;

        for (const workspace of workspaces) {
          expect((yield* target.head({ workspace, rev: 2 })).nextOffset).toBe(ZERO_OFFSET);
          expect((yield* wake.head({ workspace, epoch: 2 })).nextOffset).toBe(ZERO_OFFSET);
        }

        yield* Deferred.succeed(release, undefined);

        for (const workspace of workspaces) {
          expect(yield* eventually(items(workspace, 2), (rows) => rows.length === 2)).toEqual([
            30, 50,
          ]);
          expect(yield* items(workspace, 1)).toEqual([3, 5]);
        }
      }),
  );

  it.effect(
    "uses an explicit old-id list and provisions wrapped outputs for an open empty input",
    () =>
      Effect.gen(function* () {
        const old = numbers("old-numbers", 1);
        const historical = { workspace: "historical" };
        const empty = { workspace: "empty" };
        yield* seed(historical.workspace);
        yield* Projection.run(old.at(historical));
        yield* (yield* Facts).create(empty);

        const next = Projection.emit({
          ...declaration,
          id: "new-numbers",
          generation: 2,
          outputs: { numbers: output, untouched: Numbers.at({ workspace: "unwrapped", rev: 2 }) },
          accepted,
          commit: Commit.transaction,
          emit: ({ item }) => ({ numbers: [item.n * 2] }),
        });

        const bindings = Projection.bindings(old).pipe(Effect.map((stored) => [...stored, empty]));
        yield* Layer.build(Projection.attach(next, { durable: true, bindings }));
        expect(
          yield* eventually(items(historical.workspace, 2), (rows) => rows.length === 2),
        ).toEqual([6, 10]);
        const target = yield* Numbers;
        expect((yield* target.head({ ...empty, rev: 2 })).nextOffset).toBe(ZERO_OFFSET);
        expect((yield* (yield* Accepted).head({ ...empty, epoch: 2 })).nextOffset).toBe(
          ZERO_OFFSET,
        );
        expect(
          yield* target.head({ workspace: "unwrapped", rev: 2 }).pipe(Effect.flip),
        ).toMatchObject({ _tag: "StreamNotFound" });
        expect((yield* next.at(empty).commit.load(next.at(empty))).record).toEqual(Option.none());
      }),
  );

  it.effect("resumes startup work from a committed unit after the runner restarts", () =>
    Effect.gen(function* () {
      const params = { workspace: "restart" };
      const old = numbers("restart", 1, Commit.producer);
      yield* seed(params.workspace);
      yield* Projection.run(old.at(params));
      const second = yield* Deferred.make<void>();

      const paused = Projection.emit({
        ...declaration,
        id: old.id,
        generation: 2,
        outputs: { numbers: output },
        accepted,
        commit: Commit.producer,
        emit: ({ item }) =>
          item.id === "b"
            ? Deferred.succeed(second, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.succeed({ numbers: [item.n] }),
      });

      const scope = yield* Scope.fork(yield* Scope.Scope);
      yield* Layer.build(Projection.attach(paused, { durable: true, limit: 1 })).pipe(
        Scope.provide(scope),
      );
      yield* Deferred.await(second);
      expect(yield* Projection.bindings(paused, { generation: 2 })).toEqual([params]);
      expect(yield* items(params.workspace, 2)).toEqual([3]);
      yield* Scope.close(scope, Exit.void);

      const next = numbers(old.id, 2, Commit.producer);
      yield* Layer.build(Projection.attach(next, { durable: true }));
      expect(yield* eventually(items(params.workspace, 2), (rows) => rows.length === 2)).toEqual([
        3, 5,
      ]);
    }),
  );

  it.effect("keeps retired and ended bindings fenced even if an older generation lists them", () =>
    Effect.gen(function* () {
      const old = numbers("fences", 1);
      const next = numbers(old.id, 2);
      const retired = { workspace: "retired" };
      const ended = { workspace: "ended" };
      const live = { workspace: "live" };

      for (const params of [retired, ended, live]) {
        yield* seed(params.workspace);
        yield* Projection.run(old.at(params));
      }

      yield* Projection.retire(next, retired, { generation: 2 });
      const endedBinding = next.at(ended);
      const store = yield* CheckpointStore;
      yield* store.compareAndSet(endedBinding, "0", {
        identity: { inputs: {} },
        inputs: {},
        adapters: {},
        ended: true,
      });

      yield* Layer.build(Projection.attach(next, { durable: true }));
      expect(yield* eventually(items(live.workspace, 2), (rows) => rows.length === 2)).toEqual([
        3, 5,
      ]);

      const target = yield* Numbers;
      const wake = yield* Accepted;

      for (const params of [retired, ended]) {
        expect(yield* target.head({ ...params, rev: 2 }).pipe(Effect.flip)).toMatchObject({
          _tag: "StreamNotFound",
        });
        expect(yield* wake.head({ ...params, epoch: 2 }).pipe(Effect.flip)).toMatchObject({
          _tag: "StreamNotFound",
        });
      }
    }),
  );

  it.effect("attaches at a raised generation when the only checkpoint uses a legacy slot", () =>
    Effect.gen(function* () {
      const params = { workspace: "legacy-slot" };
      yield* seed(params.workspace);
      const next = numbers("legacy-slot", 2);
      const { params: encoded } = next.at(params);
      const source = Facts.at(params);
      const head = yield* (yield* Facts).head(params);

      const slot = encodeKey([
        "streamsy.projection.v1",
        next.id,
        "3",
        "1",
        canonicalParams(encoded),
      ]);

      const envelope = Schema.fromJsonString(
        Schema.Struct({ version: Schema.Literal(1), record: CheckpointRecord }),
      );

      yield* (yield* OwnerRecords).write(
        slot,
        yield* Schema.encodeEffect(envelope)({
          version: 1,
          record: {
            identity: { inputs: { facts: encodeKey([source.id, source.contentType]) } },
            inputs: { facts: head.nextOffset },
            adapters: {},
          },
        }),
      );
      const store = yield* CheckpointStore;

      const listed = yield* store.list?.(next.id) ?? Effect.die("Expected checkpoint listing");
      expect(listed).toEqual([{ generation: 1, params: encoded }]);
      expect((yield* store.load(next.at(params))).record).toEqual(Option.none());

      yield* Layer.build(Projection.attach(next, { durable: true }));
      expect(yield* eventually(items(params.workspace, 2), (rows) => rows.length === 2)).toEqual([
        3, 5,
      ]);
    }),
  );

  it.effect(
    "keeps the first-write protection when an explicit list selects an occupied output",
    () =>
      Effect.gen(function* () {
        const params = { workspace: "occupied-at-startup" };
        const next = numbers("occupied-at-startup", 1);
        yield* seed(params.workspace);
        yield* Projection.run(next.at(params));
        const bindings = yield* Projection.bindings(next);
        yield* Projection.forget(next.at(params));

        const selected = yield* ProjectionWakeup.attach(next, {
          bindings: Effect.succeed(bindings),
        });

        expect(Option.isSome(yield* selected.next)).toBe(true);
        yield* selected.run;
        expect(yield* selected.deferred).toBe(1);
        expect((yield* next.at(params).commit.load(next.at(params))).record).toEqual(Option.none());
        expect(yield* items(params.workspace, 1)).toEqual([3, 5]);
      }),
  );

  it.effect("arms the alarm at acquisition and rebuilds without another commit", () =>
    Effect.gen(function* () {
      const params = { workspace: "alarm" };
      const old = numbers("alarm", 1);
      yield* seed(params.workspace);
      yield* Projection.run(old.at(params));
      let armed = Option.none<number>();
      const ctx = yield* Effect.context<Layer.Success<typeof host>>();

      const scheduler = yield* makeWakeups(
        Context.add(ctx, AlarmSlot, {
          current: Effect.sync(() => armed),
          arm: (at) =>
            Effect.sync(() => {
              armed = Option.some(at);
            }),
          clear: Effect.sync(() => {
            armed = Option.none();
          }),
        }),
        [ProjectionWakeup.attach(numbers(old.id, 2))],
      );

      expect(Option.isSome(armed)).toBe(true);
      expect(yield* items(params.workspace, 2)).toEqual([]);
      yield* scheduler.alarm;
      expect(yield* items(params.workspace, 2)).toEqual([3, 5]);
      expect(armed).toEqual(Option.none());
    }),
  );

  it.effect("falls back to live commits if implicit inventory is unsupported", () =>
    Effect.gen(function* () {
      const params = { workspace: "unsupported" };
      yield* seed(params.workspace);
      const old = numbers("unsupported", 1);
      const next = numbers(old.id, 2);
      yield* Projection.run(old.at(params));
      const store = yield* CheckpointStore;

      const unsupported = {
        ...store,
        list: () =>
          Effect.fail(
            new ProjectionFault({
              phase: "load",
              reason: "unsupported-composition",
              message: "Cannot decode hashed keys",
            }),
          ),
      };

      const wakeup = yield* ProjectionWakeup.attach(next).pipe(
        Effect.provideService(CheckpointStore, unsupported),
      );

      expect(yield* wakeup.next.pipe(Effect.provideService(CheckpointStore, unsupported))).toEqual(
        Option.none(),
      );
      yield* (yield* Facts).append(params, [{ id: "c", n: 7 }]);
      yield* wakeup.run;
      expect(yield* items(params.workspace, 2)).toEqual([3, 5, 7]);

      const explicit = yield* ProjectionWakeup.attach(numbers("explicit", 3), {
        bindings: Effect.succeed([params]),
      });

      expect(
        Option.isSome(
          yield* explicit.next.pipe(Effect.provideService(CheckpointStore, unsupported)),
        ),
      ).toBe(true);
      yield* explicit.run;
      expect(yield* items(params.workspace, 3)).toEqual([3, 5, 7]);
    }),
  );

  it.effect("keeps live commits running if another generation holds a malformed checkpoint", () =>
    Effect.gen(function* () {
      const params = { workspace: "malformed" };
      yield* seed(params.workspace);
      const projection = numbers("malformed", 1);
      yield* Projection.run(projection.at(params));

      yield* (yield* OwnerRecords).write(
        recordKey({ id: projection.id, generation: 7, params: { workspace: "bad" } }),
        "not a checkpoint",
      );
      yield* Layer.build(Projection.attach(projection, { durable: true }));
      yield* (yield* Facts).append(params, [{ id: "c", n: 7 }]);

      expect(
        yield* eventually(items(params.workspace, 1), (values) => values.length === 3),
      ).toEqual([3, 5, 7]);
    }),
  );

  it.effect("keeps live commits running if the explicit bindings option fails", () =>
    Effect.gen(function* () {
      const params = { workspace: "failed-list" };
      yield* seed(params.workspace);
      const old = numbers("failed-list", 1);
      yield* Projection.run(old.at(params));

      const wakeup = yield* ProjectionWakeup.attach(numbers(old.id, 2), {
        bindings: Effect.fail("list unavailable"),
      });

      expect(yield* wakeup.next).toEqual(Option.none());
      yield* (yield* Facts).append(params, [{ id: "c", n: 7 }]);
      yield* wakeup.run;
      expect(yield* items(params.workspace, 2)).toEqual([3, 5, 7]);
    }),
  );
});

class Singleton extends Streamsy.json<Singleton>()("Rebuild/Singleton", "rebuild-singleton", {
  params: {},
  schema: Schema.Finite,
}) {}

layer(withFamilies(MemoryStorage.layerProtocol(), Singleton))((it) => {
  it.effect(
    "attaches a custom Commit without a CheckpointStore and accepts an explicit singleton",
    () =>
      Effect.gen(function* () {
        const source = yield* Singleton;
        yield* source.create({});
        yield* source.append({}, [1]);
        const seen = yield* Deferred.make<void>();
        const saved = yield* Ref.make<Loaded>({ record: Option.none(), token: "0" });

        const custom = Projection.process({
          id: "custom",
          params: {},
          inputs: { source: Singleton },
          commit: Commit.make<void>({
            name: "custom",
            guarantee: "at-least-once",
            load: () => Ref.get(saved),
            apply: (pending) =>
              pending.run.pipe(
                Effect.andThen(Ref.set(saved, { record: Option.some(pending.next), token: "1" })),
              ),
          }),
          process: () => Deferred.succeed(seen, undefined).pipe(Effect.asVoid),
        });

        expect(
          yield* ProjectionWakeup.attach(custom).pipe(
            Effect.flatMap((wakeup) => wakeup.next),
            Effect.scoped,
          ),
        ).toEqual(Option.none());
        yield* Layer.build(Projection.attach(custom, { limit: 1, bindings: Effect.succeed([{}]) }));
        yield* Deferred.await(seen);
      }),
  );
});

layer(withFamilies(layerSqlite(), Singleton))((it) => {
  it.effect("replays a singleton's history without a checkpoint inventory entry", () =>
    Effect.gen(function* () {
      const source = yield* Singleton;
      yield* source.create({});
      yield* source.append({}, [3, 5]);

      const sum = Projection.fold({
        id: "singleton",
        params: {},
        inputs: { source: Singleton },
        state: { schema: Schema.Finite, initial: 0 },
        fold: (total, { item }) => total + item,
      });

      yield* Layer.build(Projection.attach(sum));
      const snapshot = yield* eventually(Projection.read(sum.at({})), (value) => value.state === 8);
      expect(snapshot.state).toBe(8);
    }),
  );
});
