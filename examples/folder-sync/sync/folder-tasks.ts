import { Effect, Semaphore } from "effect";

/** Serialize each folder's start and catch-ups, while allowing different folders to progress. */
export const folderTasks = <E, R>(
  start: (folderId: string) => Effect.Effect<void, E, R>,
  pull: (folderId: string) => Effect.Effect<void, E, R>,
) =>
  Effect.sync(() => {
    /** One gate for each folder that `addFolder` has seen. */
    const gates = new Map<string, Semaphore.Semaphore>();
    /** The folders whose start has completed. */
    const ready = new Set<string>();

    const addFolder = Effect.fn("Sync.addFolder")(function* (folderId: string) {
      let gate = gates.get(folderId);

      if (gate === undefined) {
        gate = Semaphore.makeUnsafe(1);
        gates.set(folderId, gate);
      }

      yield* gate.withPermit(
        Effect.suspend(() =>
          ready.has(folderId)
            ? Effect.void
            : Effect.tap(start(folderId), () => Effect.sync(() => ready.add(folderId))),
        ),
      );
    });

    /** Read one ready folder to its tail. It waits for a start in progress; other folders skip. */
    const catchUp = Effect.fn("Sync.catchUp")(function* (folderId: string) {
      const gate = gates.get(folderId);

      if (gate === undefined) return;
      yield* gate.withPermit(
        Effect.suspend(() => (ready.has(folderId) ? pull(folderId) : Effect.void)),
      );
    });

    return { ready, addFolder, catchUp };
  });
