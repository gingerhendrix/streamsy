import { expect, test } from "bun:test";
import { Context, Effect, Option, Schema } from "effect";
import { Streamsy, MemoryStorage } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import {
  CheckpointRecord,
  Commit,
  Projection,
  ProjectionFault,
  type Folded,
} from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";

class Input extends Streamsy.json<Input>()("bucket/Input", "bucket/input", {
  params: {},
  schema: Schema.Number,
}) {}

class Bucket extends Context.Service<
  Bucket,
  {
    readonly get: (
      key: string,
    ) => Effect.Effect<{ readonly body: string; readonly etag: string } | undefined>;
    readonly put: (
      key: string,
      body: string,
      ifMatch: string,
    ) => Effect.Effect<void, ProjectionFault>;
  }
>()("test/Bucket") {}

const json = Schema.fromJsonString(CheckpointRecord);

const bucket = Commit.make({
  name: "bucketSnapshot",
  guarantee: "exactly-once",
  load: (key) =>
    Effect.gen(function* () {
      const found = yield* (yield* Bucket).get(recordKey(key));

      if (found === undefined) return { record: Option.none<CheckpointRecord>(), token: "0" };

      const record = yield* Schema.decodeEffect(json)(found.body).pipe(
        Effect.mapError(
          (cause) =>
            new ProjectionFault({
              phase: "load",
              reason: "invalid-record",
              message: "Bad bucket snapshot",
              cause,
            }),
        ),
      );

      return { record: Option.some(record), token: found.etag };
    }),
  apply: (pending: Commit.Pending<Folded<unknown>>) =>
    Effect.gen(function* () {
      const result = yield* pending.run;

      // oxlint-disable-next-line typescript/no-misused-spread -- CheckpointRecord is plain Schema.Struct data.
      const body = yield* Schema.encodeEffect(json)({
        // oxlint-disable-next-line typescript/no-misused-spread -- CheckpointRecord is plain Schema.Struct data.
        ...pending.next,
        state: result.encoded,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProjectionFault({
              phase: "checkpoint",
              reason: "invalid-record",
              message: "Bad snapshot encoding",
              cause,
            }),
        ),
      );

      yield* (yield* Bucket).put(recordKey(pending.key), body, pending.loaded.token);
    }),
});

const declaration = Projection.fold({
  id: "bucket",
  commit: bucket,
  params: {},
  inputs: { input: Input },
  state: { schema: Schema.Number, initial: 0 },
  fold: (sum, { item }) => sum + item,
});

const bind = () => declaration.at({});

test("a bucket Commit runs, restarts and reads through its own load without CheckpointStore", () => {
  const objects = new Map<string, { body: string; etag: string }>();

  const api: typeof Bucket.Service = {
    get: (key) => Effect.sync(() => objects.get(key)),
    put: (key, body, token) =>
      Effect.gen(function* () {
        const current = objects.get(key)?.etag ?? "0";

        if (current !== token)
          return yield* new ProjectionFault({
            phase: "checkpoint",
            reason: "token-conflict",
            message: "etag moved",
          });
        objects.set(key, { body, etag: String(Number(token) + 1) });
      }),
  };

  return Effect.runPromise(
    Effect.gen(function* () {
      const input = yield* Input;
      yield* input.create({});
      yield* input.append({}, [2, 3]);
      yield* Projection.run(bind());
      expect((yield* Projection.read(bind())).state).toBe(5);
      yield* input.append({}, [7]);
      yield* Projection.run(bind());
      expect((yield* Projection.read(bind())).state).toBe(12);
      expect((yield* Projection.read(declaration.at({}))).state).toBe(12);
      const before = [...objects.values()];
      yield* input.append({}, [11]);

      const conflict = yield* Projection.run(bind()).pipe(
        Effect.provideService(Bucket, {
          ...api,
          put: (key, body, _token) => api.put(key, body, "0"),
        }),
        Effect.result,
      );

      expect(conflict._tag).toBe("Failure");

      if (conflict._tag === "Failure") expect(conflict.failure.reason).toBe("token-conflict");
      expect([...objects.values()]).toEqual(before);
      const broken = { ...bind(), process: () => Effect.fail("step failed") };
      const failure = yield* Projection.run(broken).pipe(Effect.result);
      expect(failure).toMatchObject({ _tag: "Failure", failure: "step failed" });
      expect([...objects.values()]).toEqual(before);
      const forgotten = yield* Projection.forget(bind()).pipe(Effect.result);
      expect(forgotten).toMatchObject({
        _tag: "Failure",
        failure: { reason: "unsupported-composition" },
      });
    }).pipe(
      Effect.provideService(Bucket, api),
      Effect.provide(Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Input))),
    ),
  );
});
