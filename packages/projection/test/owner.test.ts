import { Protocol } from "@streamsy/core/protocol";
import { expect, test } from "bun:test";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { Streamsy } from "@streamsy/core";
import { Checkpoints, Output, Projection } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";

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
const local = Memory.layerMemory();
const foreign = Memory.layerMemory();
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
  test(`declared outputs ${sameOwner ? "share their checkpoint owner" : "reject a foreign owner before any write"}`, async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const checkpoints = yield* Checkpoints;
        let processed = false;
        const projection = Projection.outputs({
          id: `declared-${sameOwner}`,
          params: {},
          inputs: { input: Input.at({}) },
          outputs: {
            local: Output.stream(Schema.Finite, { stream: Local.at(params) }),
            foreign: sameOwner
              ? Output.stream(Schema.Finite, { stream: Local.at({ id: "b" }) })
              : Output.stream(Schema.Finite, { stream: Foreign.at(params) }),
          },
          process: () =>
            Effect.sync(() => {
              processed = true;
              return { local: [2], foreign: [3] };
            }),
        }).at({});
        const result = yield* Projection.run(projection).pipe(Effect.result);
        expect(result._tag).toBe(sameOwner ? "Success" : "Failure");
        expect(processed).toBe(sameOwner);
        const loaded = yield* checkpoints.load(projection);
        expect(Option.isSome(loaded.record)).toBe(sameOwner);
        if (result._tag === "Failure") {
          expect(result.failure.reason).toBe("unsupported-composition");
          expect((yield* (yield* Local).head(params).pipe(Effect.result))._tag).toBe("Failure");
          expect((yield* (yield* Foreign).head(params).pipe(Effect.result))._tag).toBe("Failure");
        }
      }).pipe(Effect.provide(host)),
    );
  });
}

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
  test(`atomic ${method} rejects a foreign owner and rolls back prior local writes`, async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const own = yield* Local;
        const other = yield* Foreign;
        const checkpoints = yield* Checkpoints;
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
        const projection = Projection.atomic({
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
      }).pipe(Effect.provide(host)),
    );
  });
}

test("declared state on a foreign State owner fails before processing", async () => {
  const foreignState = Layer.effect(Projection.State, Projection.State).pipe(
    Layer.provide(Layer.fresh(foreign)),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      let processed = false;
      const projection = Projection.outputs({
        id: "foreign-state",
        params: {},
        inputs: { input: Input.at({}) },
        outputs: {},
        state: { schema: Schema.Finite, initial: 0 },
        process: () =>
          Effect.sync(() => {
            processed = true;
            return { state: 1 };
          }),
      }).at({});
      const result = yield* Projection.run({ ...projection }).pipe(
        Effect.provide(foreignState),
        Effect.result,
      );
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("unsupported-composition");
      expect(processed).toBe(false);
      expect((yield* (yield* Checkpoints).load(projection)).token).toBe("0");
    }).pipe(Effect.provide(host)),
  );
});

test("a pinned stream on a foreign owner fails before pinning or creating it", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const projection = Projection.outputs({
        id: "foreign-pinned",
        params: {},
        inputs: { input: Input.at({}) },
        outputs: { output: Output.stream(Schema.Finite, { stream: Foreign.at(params) }) },
        process: () => Effect.succeed({ output: [1] }),
      }).at({});
      const result = yield* Projection.run(projection).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("unsupported-composition");
      expect((yield* (yield* Checkpoints).load(projection)).token).toBe("0");
      expect((yield* (yield* Foreign).head(params).pipe(Effect.result))._tag).toBe("Failure");
    }).pipe(Effect.provide(host)),
  );
});
