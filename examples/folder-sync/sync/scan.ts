import { Clock, Effect, Schema } from "effect";
import { filePath, writable, type SyncContext } from "./context.ts";
import { isEcho } from "./decisions.ts";
import type { Install } from "./install.ts";
import { captureLocal, localFiles, SkippedLocal } from "./local.ts";

/**
 * Walk every folder root. A writable folder queues each local change. A
 * read-only folder puts back the server copy of each path that differs.
 * The caller holds the gate.
 */
export const scanner = (context: SyncContext, { observe, refresh }: Install) => {
  const { path, state, root, roots, warn } = context;

  const scanReadOnly = Effect.fn("Sync.scanReadOnly")(function* (folderId: string, file: string) {
    const known = yield* state.head(folderId, file);

    const local = yield* captureLocal(root, filePath(context, folderId, file)).pipe(
      Effect.catchIf(Schema.is(SkippedLocal), (error) =>
        warn(error.path, error.message).pipe(Effect.as(null)),
      ),
    );

    if (!isEcho(local?.hash ?? null, known)) yield* refresh(folderId, file);
  });

  const scan = Effect.fn("Sync.scan")(function* () {
    for (const [folderId, relative] of roots) {
      const files = new Set([
        ...(yield* localFiles(path.join(root, relative), warn)),
        ...(yield* state.paths(folderId)),
      ]);

      for (const file of files) {
        if (writable(context, folderId)) yield* observe(folderId, file);
        else yield* scanReadOnly(folderId, file);
      }
    }

    yield* state.set("lastScan", String(yield* Clock.currentTimeMillis));
  });

  return { scan };
};
