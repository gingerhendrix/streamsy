import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Deferred, Effect, Layer, Option, Schedule, Schema } from "effect";
import { StreamAddress, Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { memoryHost } from "./support/memory-host.ts";

class Files extends Streamsy.json<Files>()("Attach/Files", "folders/:folderId/files", {
  params: { folderId: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("Attach/Wake", "wake", { params: {} }) {}

class UserWake extends Streamsy.wake<UserWake>()("Attach/UserWake", "users/:user/wake", {
  params: { user: Schema.String },
}) {}

const filesWake = Projection.wake({
  id: "files-wake",
  params: { folderId: Schema.String },
  input: Files,
  output: Wake,
});

/** `Files` does not give `user`, so a write cannot pick one binding. */
const userWake = Projection.wake({
  id: "user-wake",
  params: { user: Schema.String, folderId: Schema.String },
  input: Files,
  output: UserWake,
});

const docs = { folderId: "docs" };

const notes = { folderId: "notes" };

/** One message for each read page, so each unit holds one source write. */
const host = () => {
  const local = memoryHost({ readLimit: 1 });

  return Protocol.bind(local, Streamsy.group(Files, Wake, UserWake)).pipe(
    Layer.provideMerge(local),
  );
};

/** The wake rows as `[operation, key]`, in order. */
const rows = Effect.gen(function* () {
  const { items } = yield* (yield* Wake).collect({});

  return Array.flatMap(items, (item) =>
    StreamAddress.isChange(item) ? [[item.headers.operation, item.key] as const] : [],
  );
}).pipe(Effect.catchTag("StreamNotFound", () => Effect.succeed([])));

/** Let the forked runner work until `check` holds. */
const eventually = <A, E, R>(read: Effect.Effect<A, E, R>, check: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 1_000; turn += 1) {
      const value = yield* read;

      if (check(value)) return value;
      yield* Effect.yieldNow;
    }

    return yield* read;
  });

layer(host())((it) => {
  it.effect("a projection whose input does not give every param cannot attach", () =>
    Effect.sync(() => {
      expect(() => Projection.attach(userWake)).toThrow(
        "Projection user-wake cannot attach: input source does not give param user",
      );
      expect(() => ProjectionWakeup.attach(userWake)).toThrow(RangeError);
    }),
  );
});

layer(Layer.provideMerge(Projection.attach(filesWake), host()))((it) => {
  it.effect("Projection.attach runs the binding of each written family member", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);
      yield* files.create(notes);
      yield* files.append(notes, ["b"]);

      const key = (folder: { readonly folderId: string }) => Files.at(folder).id;

      expect(yield* eventually(rows, (value) => value.length === 2)).toEqual([
        ["upsert", key(docs)],
        ["upsert", key(notes)],
      ]);

      yield* files.remove(docs);
      expect((yield* eventually(rows, (value) => value.length === 3))[2]).toEqual([
        "delete",
        key(docs),
      ]);
    }),
  );
});

layer(host())((it) => {
  it.effect("an attach wakeup answers now after a commit and drains the set", () =>
    Effect.gen(function* () {
      const wakeup = yield* ProjectionWakeup.attach(filesWake, { limit: 2 });
      expect(wakeup.name).toBe("streamsy/projection/files-wake");

      // Acquiring the wakeup subscribes before the first write.
      expect(Option.isNone(yield* wakeup.next)).toBe(true);

      const files = yield* Files;
      yield* files.create(docs);
      expect(Option.isSome(yield* wakeup.next)).toBe(true);
      // A new member runs on its create. Its input is empty, so it writes no row.
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);

      yield* files.append(docs, ["a"]);
      yield* files.append(docs, ["b"]);
      yield* files.append(docs, ["c"]);
      yield* files.create(notes);
      yield* files.append(notes, ["d"]);

      // Three units of docs and a limit of two: docs stays in the set.
      expect(yield* wakeup.run).toEqual({ more: true });
      expect(Option.isSome(yield* wakeup.next)).toBe(true);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);

      yield* files.remove(notes);
      yield* wakeup.run;

      const key = (folder: { readonly folderId: string }) => Files.at(folder).id;

      expect(yield* rows).toEqual([
        ["upsert", key(docs)],
        ["upsert", key(docs)],
        ["upsert", key(notes)],
        ["upsert", key(docs)],
        ["delete", key(notes)],
      ]);
    }),
  );
});

layer(host())((it) => {
  it.effect(
    "the shared drain retries failed bindings, runs peers, and keeps commits during backoff",
    () =>
      Effect.gen(function* () {
        const firstFailure = yield* Deferred.make<void>();
        const peerDone = yield* Deferred.make<void>();
        const recovered = yield* Deferred.make<void>();
        let attempts = 0;
        let peers = 0;

        const projection = Projection.process({
          id: "retrying-attachment",
          params: { folderId: Schema.String },
          inputs: { files: Files },
          commit: Commit.atLeastOnce,
          process: (_batch, unit) =>
            Effect.suspend(() => {
              if (unit.params.folderId === "docs") {
                attempts += 1;

                return attempts < 3
                  ? Deferred.succeed(firstFailure, undefined).pipe(
                      Effect.andThen(Effect.fail("offline")),
                    )
                  : Deferred.succeed(recovered, undefined).pipe(Effect.asVoid);
              }

              peers += 1;

              return Deferred.succeed(peerDone, undefined).pipe(Effect.asVoid);
            }),
        });

        yield* Layer.build(Projection.attach(projection, { retry: Schedule.spaced("1 second") }));
        const files = yield* Files;
        yield* files.create(docs);
        yield* files.append(docs, ["a"]);
        yield* Deferred.await(firstFailure);
        yield* files.create(notes);
        yield* files.append(notes, ["b"]);
        yield* TestClock.adjust("1 second");
        yield* Deferred.await(peerDone);
        expect(attempts).toBe(2);
        expect(peers).toBe(1);
        yield* TestClock.adjust("1 second");
        yield* Deferred.await(recovered);
        expect(attempts).toBe(3);
        expect(peers).toBe(1);
      }),
  );
});

layer(host())((it) => {
  it.effect("a source removed before its binding first runs drops the binding", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);

      // The projection attaches to data that exists, and the source goes before any run.
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      yield* files.remove(docs);
      expect(Option.isSome(yield* wakeup.next)).toBe(true);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);
      // The binding wrote no wake row, so it has no delete to write.
      expect(yield* rows).toEqual([]);
    }),
  );
});

layer(host())((it) => {
  it.effect("a create, write and remove before the first drain drop the binding", () =>
    Effect.gen(function* () {
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);
      yield* files.remove(docs);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);
      expect(yield* rows).toEqual([]);
    }),
  );
});

layer(host())((it) => {
  it.effect("an empty source removed after a run with no checkpoint drops the binding", () =>
    Effect.gen(function* () {
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      const files = yield* Files;
      yield* files.create(docs);
      // The first run reads nothing, so it saves no checkpoint.
      expect(yield* wakeup.run).toEqual({ more: false });
      yield* files.remove(docs);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);
    }),
  );
});

layer(host())((it) => {
  it.effect("a dropped binding does not fail the pass of a live neighbour", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(docs);
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      yield* files.remove(docs);
      yield* files.create(notes);
      yield* files.append(notes, ["b"]);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);
      expect(yield* rows).toEqual([["upsert", Files.at(notes).id]]);
    }),
  );
});

layer(Layer.provideMerge(Projection.attach(filesWake), host()))((it) => {
  it.effect("Projection.attach keeps serving a neighbour after a dropped binding", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.remove(docs);
      // Let the forked runner finish its pass over the removed source.
      yield* eventually(Effect.void, () => false);
      yield* files.create(notes);
      yield* files.append(notes, ["b"]);
      // A failed pass would put the loop on retry, and the paused test clock
      // would hold the neighbour's write forever.
      expect(yield* eventually(rows, (value) => value.length === 1)).toEqual([
        ["upsert", Files.at(notes).id],
      ]);
    }),
  );
});
