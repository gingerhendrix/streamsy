import { Offset, Streamsy } from "@streamsy/core";
import { BunCrypto, BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Layer, Schema } from "effect";
import { testDirectory } from "../../scripts/test-directory.ts";
import { Files } from "../../server/streams.ts";
import { RemoteFailure, SyncRemote } from "../../sync/client.ts";
import { openContext } from "../../sync/context.ts";
import { cursorKey } from "../../sync/pull.ts";
import { SyncState, stateLayer } from "../../sync/state.ts";
import { waker } from "../../sync/wake.ts";

const offset = (n: number) =>
  Schema.decodeSync(Offset)(`${String(n).padStart(16, "0")}_${"0".repeat(16)}`);

/** One wake page: a row for each folder at its tail. */
const wakePage = (rows: ReadonlyArray<readonly [string, string]>, cursor: string) =>
  Effect.gen(function* () {
    const messages = yield* Schema.decodeUnknownEffect(Schema.Array(Streamsy.WakeMessage))(
      Array.map(rows, ([folderId, tail]) => {
        const stream = Files.at({ folderId }).id;

        return {
          type: "stream",
          key: stream,
          value: { stream, offset: tail },
          headers: { operation: "upsert" },
        };
      }),
    );

    return {
      rows: messages,
      cursor: yield* Schema.decodeEffect(Offset)(cursor),
      upToDate: true,
      reset: false,
    };
  });

/** A remote whose wake stream always answers `page`. Nothing else is read. */
const remoteOf = (page: ReturnType<typeof wakePage>) =>
  SyncRemote.of({
    folders: () => Effect.die("unused"),
    page: () => Effect.die("unused"),
    wake: () => page,
    tail: () => Effect.die("unused"),
    body: () => Effect.die("unused"),
    head: () => Effect.die("unused"),
    upload: () => Effect.die("unused"),
    push: () => Effect.die("unused"),
  });

const stop = new RemoteFailure({ message: "stopped", retryable: false, status: 0 });

layer(Layer.mergeAll(BunServices.layer, BunCrypto.layer), { excludeTestServices: true })((it) => {
  it.effect(
    "a stop before the wake cursor save reads the rows again; folders already read are skipped and none is lost",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-wake-order-");

        const state = yield* Layer.build(stateLayer(`${directory}/state.sqlite`)).pipe(
          Effect.flatMap((context) => SyncState.pipe(Effect.provideContext(context))),
        );

        const page = wakePage(
          [
            ["a", offset(2)],
            ["b", offset(3)],
            ["unselected", offset(9)],
          ],
          offset(7),
        );

        const context = yield* openContext({
          server: "http://unused",
          directory,
          folders: ["a", "b", "c"],
          writable: [],
        }).pipe(
          Effect.provideService(SyncState, state),
          Effect.provideService(SyncRemote, remoteOf(page)),
        );

        const read: Array<string> = [];
        // The first read of `b` stops the run, as a crash would.
        let stopB = true;
        // Then the first wake cursor save stops the run.
        let stopSave = true;
        let cursorReads = 0;

        const ready = new Set(["a", "b", "c"]);

        // Like `folderTasks`, a catch-up of a folder that discovery did not start does nothing.
        const catchUp = (folderId: string) =>
          Effect.gen(function* () {
            if (!ready.has(folderId)) return yield* Effect.void;
            read.push(folderId);

            if (folderId === "b" && stopB) {
              stopB = false;

              return yield* stop;
            }

            // The folder read saves its cursor at the row's tail.
            yield* state.set(cursorKey(folderId), folderId === "a" ? offset(2) : offset(3));

            return yield* Effect.void;
          });

        const saving = SyncState.of({
          ...state,
          cursors: () => {
            cursorReads += 1;

            return state.cursors();
          },
          get: (key) => {
            expect(key.startsWith("cursor:")).toBe(false);

            return state.get(key);
          },
          set: (key, value) => {
            if (key === "wakeCursor" && stopSave) {
              stopSave = false;

              return Effect.die(stop);
            }

            return state.set(key, value);
          },
        });

        const { follow } = waker({ ...context, state: saving }, { ready, catchUp });

        // Stop after `a` is read and before `b` is.
        expect(yield* Effect.flip(follow())).toBe(stop);
        expect(yield* state.get("wakeCursor")).toBeUndefined();

        // Stop after both folder cursors are saved and before the wake cursor is.
        yield* Effect.exit(follow());
        expect(read).toEqual(["a", "b", "b"]);
        expect(yield* state.get("wakeCursor")).toBeUndefined();

        // The rows come again. Every folder is at its row, so nothing is read twice.
        yield* follow();
        expect(read).toEqual(["a", "b", "b"]);
        expect(yield* state.get("wakeCursor")).toBe(offset(7));
        expect(cursorReads).toBe(3);
        expect(yield* state.get(cursorKey("a"))).toBe(offset(2));
        expect(yield* state.get(cursorKey("b"))).toBe(offset(3));
      }),
  );
});
