import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Exit, Layer, Option, Schema, Scope } from "effect";
import { Storage, StreamAddress, Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import type { Wakeup } from "@streamsy/serve/cloudflare";
import { Projection, ProjectionWakeup } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";

class Files extends Streamsy.json<Files>()("Wakeup/Files", "users/:user/folders/:folderId/files", {
  params: { user: Schema.String, folderId: Schema.String },
  schema: Schema.String,
}) {}

class UserWake extends Streamsy.wake<UserWake>()("Wakeup/UserWake", "users/:user/wake", {
  params: { user: Schema.String },
}) {}

const filesWake = Projection.wake({
  id: "files-wake",
  params: { user: Schema.String, folderId: Schema.String },
  input: Files,
  output: UserWake,
});

const folder = { user: "ann", folderId: "docs" };

/** One message for each read page, so each unit holds one source write. */
const host = () => {
  const local = memoryHost({ readLimit: 1 });

  return Protocol.bind(local, Streamsy.group(Files, UserWake)).pipe(Layer.provideMerge(local));
};

const ops = Effect.gen(function* () {
  const { items } = yield* (yield* UserWake).collect({ user: "ann" });

  return Array.flatMap(items, (item) =>
    StreamAddress.isChange(item) ? [item.headers.operation] : [],
  );
});

layer(host())((it) => {
  it.effect("next answers now while a binding has work, and run drains it", () =>
    Effect.gen(function* () {
      const wakeup = yield* ProjectionWakeup.attach(filesWake, { limit: 2 });
      expect(wakeup.name).toBe("streamsy/projection/files-wake");

      // The source does not exist yet, so there is nothing to run.
      expect(Option.isNone(yield* wakeup.next)).toBe(true);

      const files = yield* Files;
      yield* files.create(folder);
      expect(Option.isSome(yield* wakeup.next)).toBe(true);

      yield* files.append(folder, ["a"]);
      yield* files.append(folder, ["b"]);
      yield* files.append(folder, ["c"]);
      expect(Option.isSome(yield* wakeup.next)).toBe(true);

      // Three units and a limit of two: the run reports more work.
      expect(yield* wakeup.run).toEqual({ more: true });
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(Option.isNone(yield* wakeup.next)).toBe(true);
      expect(yield* ops).toEqual(["upsert", "upsert", "upsert"]);
    }),
  );
});

layer(host())((it) => {
  it.effect("next sees a close with no body and a removed source", () =>
    Effect.gen(function* () {
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      const files = yield* Files;
      yield* files.create(folder);
      yield* files.append(folder, ["a"]);

      yield* wakeup.run;
      expect(Option.isNone(yield* wakeup.next)).toBe(true);

      yield* files.append(folder, [], { close: true });
      expect(Option.isSome(yield* wakeup.next)).toBe(true);
      yield* wakeup.run;
      expect(Option.isNone(yield* wakeup.next)).toBe(true);

      yield* files.remove(folder);
      expect(Option.isSome(yield* wakeup.next)).toBe(true);
      yield* wakeup.run;
      expect(Option.isNone(yield* wakeup.next)).toBe(true);
      expect(yield* ops).toEqual(["upsert", "upsert", "delete"]);
    }),
  );
});

layer(host())((it) => {
  it.effect("the definition fits the Cloudflare Wakeup shape", () =>
    Effect.gen(function* () {
      const wakeup: Wakeup<Files | UserWake | import("@streamsy/projection").CheckpointStore> =
        yield* ProjectionWakeup.attach(filesWake);

      expect(wakeup.retry).toBeDefined();
    }),
  );
});

layer(host())((it) => {
  it.effect(
    "closing a wakeup scope releases its commit subscription while storage stays alive",
    () =>
      Effect.gen(function* () {
        const storage = yield* Storage;
        let releases = 0;

        const observed = Storage.of({
          ...storage,
          committed: Effect.tap(storage.committed, () =>
            Effect.addFinalizer(() =>
              Effect.sync(() => {
                releases += 1;
              }),
            ),
          ),
        });

        const scope = yield* Scope.fork(yield* Scope.Scope);

        // In-memory only: a durable mark would outlive the scope by design.
        const first = yield* ProjectionWakeup.attach(filesWake, { durable: false }).pipe(
          Effect.provideService(Storage, observed),
          Scope.provide(scope),
        );

        yield* (yield* Files).create(folder);
        expect(Option.isSome(yield* first.next)).toBe(true);
        yield* Scope.close(scope, Exit.void);
        expect(releases).toBe(1);
        const second = yield* ProjectionWakeup.attach(filesWake, { durable: false });
        expect(Option.isNone(yield* second.next)).toBe(true);
        yield* (yield* Files).append(folder, ["after shutdown"]);
        expect(Option.isSome(yield* second.next)).toBe(true);
        yield* second.run;
        expect(yield* ops).toEqual(["upsert"]);
      }),
  );
});
