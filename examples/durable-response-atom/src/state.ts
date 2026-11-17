import { Effect, Schedule, Stream } from "effect";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { applyBatch, emptySnapshot, persist } from "@streamsy/durable-response-contract";
import { modelCalls } from "@streamsy/durable-response-contract/client";
import { atomRuntime, ResponseReader } from "./runtime.ts";

export const responseIdAtom = Atom.make("");

export const readerIdAtom = Atom.make("");

export const promptAtom = Atom.make("Explain durable streams");

export const modeAtom = Atom.make<"connected" | "dropped">("connected");

export const snapshotAtom = Atom.make(emptySnapshot);

export const resumedAtAtom = Atom.make("-1");

export const noticeAtom = Atom.make("");

export const busyAtom = Atom.make(false);

/** Its dependencies own the read fiber. Changing mode interrupts and releases the transport. */
export const readAtom = atomRuntime.atom((get) => {
  const id = get(responseIdAtom);
  const mode = get(modeAtom);

  if (!id || mode === "dropped") return Stream.empty;

  return Stream.unwrap(
    Effect.gen(function* () {
      const registry = yield* AtomRegistry.AtomRegistry;
      const initial = registry.get(snapshotAtom);

      if (initial.closed) return Stream.empty;
      registry.set(resumedAtAtom, initial.offset);
      const reader = yield* ResponseReader;

      return reader.read(id, initial.offset).pipe(
        Stream.scanEffect(
          () => initial,
          (snapshot, batch) =>
            Effect.try({
              try: () => applyBatch(snapshot, batch),
              catch: () => "Invalid response part",
            }),
        ),
        Stream.tap((snapshot) =>
          Effect.gen(function* () {
            // Commit the checkpoint before publishing it to the UI.
            yield* persist(id, snapshot);
            registry.set(snapshotAtom, snapshot);
          }),
        ),
        Stream.catch((error) => {
          registry.set(noticeAtom, error);
          registry.set(modeAtom, "dropped");

          return Stream.empty;
        }),
      );
    }),
  );
});

const callsStream = atomRuntime.atom(
  Stream.fromEffect(modelCalls).pipe(Stream.repeat(Schedule.spaced("600 millis"))),
);

export const callsAtom = Atom.map(
  callsStream,
  AsyncResult.getOrElse(() => ({ calls: 0, interruptions: 0 })),
);
