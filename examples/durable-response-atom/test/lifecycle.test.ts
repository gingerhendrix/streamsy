import { it } from "@effect/vitest";
import { expect } from "vitest";
import { Deferred, Effect, Layer, Queue, Stream } from "effect";
import { Atom, AtomRegistry } from "effect/reactivity";
import { FetchHttpClient } from "effect/http";
import { Response } from "effect/ai";
import {
  emptySnapshot,
  encodeEntry,
  finish,
  resumeState,
  Session,
  type ReadBatch,
} from "@streamsy/durable-response-contract";
import { atomRuntime, ResponseReader } from "../src/runtime.ts";
import { modeAtom, readAtom, responseIdAtom, resumedAtAtom, snapshotAtom } from "../src/state.ts";

it.live(
  "drop interrupts the reader; reconnect uses the checkpoint, reload restores it, and unmount releases it",
  () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<ReadBatch>();
      const opened = yield* Queue.unbounded<string>();
      const cancelled = yield* Queue.unbounded<void>();
      const values = new Map<string, string>();

      const storage = {
        getItem: (key: string) => values.get(key) ?? null,
        setItem: (key: string, value: string) => {
          values.set(key, value);
        },
      };

      const layer = Layer.mergeAll(
        FetchHttpClient.layer,
        Layer.succeed(Session, storage),
        Layer.succeed(ResponseReader, {
          read: (_id, offset) =>
            Stream.unwrap(
              Queue.offer(opened, offset).pipe(
                Effect.as(
                  Stream.fromQueue(events).pipe(Stream.ensuring(Queue.offer(cancelled, undefined))),
                ),
              ),
            ),
        }),
      );

      const registry = AtomRegistry.make({
        initialValues: [
          Atom.initialValue(atomRuntime.layer, layer),
          Atom.initialValue(responseIdAtom, "one"),
        ],
      });

      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
      const release = registry.mount(readAtom);
      expect(yield* Queue.take(opened)).toBe("-1");
      const received = yield* Deferred.make<void>();

      const unsubscribe = registry.subscribe(snapshotAtom, (snapshot) => {
        if (snapshot.offset === "three") Deferred.doneUnsafe(received, Effect.void);
      });

      yield* Queue.offer(events, {
        items: [encodeEntry(Response.makePart("text-delta", { id: "a", delta: "Hello" }))],
        offset: "three",
        closed: false,
      });
      yield* Deferred.await(received);
      expect(registry.get(snapshotAtom).view.text).toBe("Hello");
      registry.set(modeAtom, "dropped");
      yield* Queue.take(cancelled);
      expect(resumeState(storage, "one").offset).toBe("three");
      registry.set(modeAtom, "connected");
      expect(yield* Queue.take(opened)).toBe("three");
      expect(registry.get(resumedAtAtom)).toBe("three");
      const finished = yield* Deferred.make<void>();

      const unfinish = registry.subscribe(snapshotAtom, (snapshot) => {
        if (snapshot.closed) Deferred.doneUnsafe(finished, Effect.void);
      });

      yield* Queue.offer(events, { items: [encodeEntry(finish())], offset: "end", closed: true });
      yield* Deferred.await(finished);
      expect(registry.get(snapshotAtom).view.status).toBe("Finished");
      expect(resumeState(storage, "one").view.text).toBe("Hello");
      expect(resumeState(storage, "one").closed).toBe(true);
      expect(emptySnapshot.view.text).toBe("");
      unsubscribe();
      unfinish();
      release();
      registry.dispose();
      yield* Queue.take(cancelled);
    }).pipe(Effect.scoped, Effect.timeout("5 seconds")),
);
