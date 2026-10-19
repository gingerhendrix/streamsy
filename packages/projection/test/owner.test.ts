import { layer } from "@effect/vitest";
import { Commit } from "@streamsy/projection";
import { Protocol } from "@streamsy/core/protocol";
import { expect } from "vitest";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, ProjectionFault, Transaction } from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";
import { memoryHost } from "./support/memory-host.ts";

class Input extends Streamsy.json<Input>()("owner/Input", "owner/input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Local extends Streamsy.json<Local>()("owner/Local", "owner/local/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}

class Foreign extends Streamsy.json<Foreign>()("owner/Foreign", "owner/foreign/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}

const params = { id: "a" };

const local = memoryHost();

const sqlite = layerSqlite();

const foreign = memoryHost();

const host = Layer.merge(
  Protocol.bind(local, Streamsy.group(Input, Local)),
  Protocol.bind(foreign, Streamsy.group(Foreign)),
).pipe(Layer.provideMerge(local));

const seed = Effect.gen(function* () {
  const input = yield* Input;
  yield* input.create({});
  yield* input.append({}, [1]);
});

// All tests exercise the public stream methods; no internal guard is supplied by the fixture.
for (const sameOwner of [true, false]) {
  layer(host)((it) => {
    it.effect(
      `declared outputs ${sameOwner ? "share their checkpoint owner" : "accept an existing foreign output"}`,
      () =>
        Effect.gen(function* () {
          yield* seed;
          const checkpoints = yield* CheckpointStore;

          if (!sameOwner) yield* (yield* Foreign).create(params);
          let processed = false;

          const projection = Projection.emit({
            commit: Commit.producer,
            id: `declared-${sameOwner}`,
            params: {},
            inputs: { input: Input.at({}) },
            outputs: {
              local: Local.at(params),
              foreign: sameOwner ? Local.at({ id: "b" }) : Foreign.at(params),
            },
            emit: () =>
              Effect.sync(() => {
                processed = true;

                return { local: [2], foreign: [3] };
              }),
          }).at({});

          const result = yield* Projection.run(projection).pipe(Effect.result);
          expect(result._tag).toBe("Success");
          expect(processed).toBe(true);
          const loaded = yield* checkpoints.load(projection);
          expect(Option.isSome(loaded.record)).toBe(true);

          const target = sameOwner
            ? (yield* Local).read({ id: "b" })
            : (yield* Foreign).read(params);

          expect((yield* Stream.runCollect(target)).flatMap((batch) => batch.items)).toEqual([3]);
        }),
    );
  });
}

const transactionHost = Layer.merge(
  Protocol.bind(sqlite, Streamsy.group(Input, Local)),
  Protocol.bind(foreign, Streamsy.group(Foreign)),
).pipe(Layer.provideMerge(sqlite));

for (const method of [
  "append",
  "create",
  "fork",
  "remove",
  "wire.append",
  "wire.create",
  "wire.fork",
  "wire.remove",
] as const) {
  layer(transactionHost)((it) => {
    it.effect(
      `transactional ${method} rejects a foreign owner and rolls back prior local writes`,
      () =>
        Effect.gen(function* () {
          yield* seed;
          const own = yield* Local;
          const other = yield* Foreign;
          const checkpoints = yield* CheckpointStore;
          yield* own.create(params);
          yield* other.create(params);
          const ref = Foreign.at(params);
          const target = Foreign.at({ id: "b" });

          const writes = {
            append: other.append(params, [9]),
            create: other.create({ id: "b" }),
            fork: other.fork({ id: "b" }, params),
            remove: other.remove(params),
            "wire.append": other.wire.append(ref.id, {
              data: new TextEncoder().encode("[9]"),
              contentType: ref.contentType,
            }),
            "wire.create": other.wire.create(target.id, { contentType: target.contentType }),
            "wire.fork": other.wire.fork(target.id, ref.id),
            "wire.remove": other.wire.remove(ref.id),
          };

          const projection = Projection.process({
            commit: Commit.transaction,
            id: method,
            params: {},
            inputs: { input: Input.at({}) },
            process: () =>
              Effect.gen(function* () {
                yield* own.append(params, [2]);
                yield* writes[method];
              }),
          }).at({});

          const result = yield* Projection.run(projection).pipe(Effect.result);
          expect(result._tag).toBe("Failure");

          if (result._tag === "Failure")
            expect(result.failure).toMatchObject({
              _tag: "ProjectionFault",
              reason: "unsupported-composition",
            });
          expect((yield* checkpoints.load(projection)).token).toBe("0");
          expect(
            (yield* Stream.runCollect(own.read(params))).flatMap((batch) => batch.items),
          ).toEqual([]);
          expect(
            (yield* Stream.runCollect(other.read(params))).flatMap((batch) => batch.items),
          ).toEqual([]);
          expect((yield* other.head({ id: "b" }).pipe(Effect.result))._tag).toBe("Failure");
          // Scope restoration: a normal foreign write remains usable after the failed pass.
          yield* other.append(params, [4]);
        }),
    );
  });
}

layer(host)((it) => {
  it.effect("a pinned stream on a foreign owner fails before pinning or creating it", () =>
    Effect.gen(function* () {
      yield* seed;

      const projection = Projection.emit({
        commit: Commit.producer,
        id: "foreign-pinned",
        params: {},
        inputs: { input: Input.at({}) },
        outputs: { output: Foreign.at(params) },
        emit: () => Effect.succeed({ output: [1] }),
      }).at({});

      const result = yield* Projection.run(projection).pipe(Effect.result);
      expect(result._tag).toBe("Failure");

      if (result._tag === "Failure") expect(result.failure.reason).toBe("invalid-output");
      expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("0");
      expect((yield* (yield* Foreign).head(params).pipe(Effect.result))._tag).toBe("Failure");
    }),
  );
});

layer(host)((it) => {
  it.effect(
    "producer settles a foreign append once after a failed checkpoint without Transaction",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const foreignOutput = yield* Foreign;
        yield* foreignOutput.create(params);

        const projection = Projection.emit({
          commit: Commit.producer,
          id: "foreign-retry",
          params: {},
          inputs: { input: Input },
          outputs: { output: Foreign.at(params) },
          emit: () => Effect.succeed({ output: [9] }),
        }).at({});

        const store = yield* CheckpointStore;

        const failure = yield* Projection.run(projection).pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: (key, token, record) =>
              record.pending === undefined
                ? Effect.fail(
                    new ProjectionFault({
                      phase: "checkpoint",
                      reason: "storage-failure",
                      message: "after foreign append",
                    }),
                  )
                : store.compareAndSet(key, token, record),
          }),
          Effect.result,
        );

        expect(failure._tag).toBe("Failure");
        yield* Projection.run(projection);
        expect(
          (yield* Stream.runCollect(foreignOutput.read(params))).flatMap((batch) => batch.items),
        ).toEqual([9]);
        expect(Option.getOrThrow((yield* store.load(projection)).record).pending).toBeUndefined();
      }).pipe(
        Effect.updateContext(
          (context: Context.Context<Input | Local | Foreign | CheckpointStore>) =>
            Context.omit(Transaction)(context),
        ),
      ),
  );
});
