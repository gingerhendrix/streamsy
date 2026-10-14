import { StorageFault, compare } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Wakeup } from "@streamsy/serve/cloudflare";
import { Effect, Option } from "effect";
import { Files, Folders, folderHead } from "../server/streams.ts";

/** Checkpoint transactions in one alarm pass for each folder. */
const UNITS_PER_RUN = 10;

const fault = (cause: unknown) =>
  new StorageFault({ operation: "folder-head.next", message: String(cause), retryable: true });

/** Folders in this object whose stored head checkpoint is behind the folder log. */
const behind = Effect.gen(function* () {
  const folders = yield* Folders;
  const files = yield* Files;
  const lagging = new Array<string>();

  for (const { folderId } of (yield* folders.collect({})).items) {
    const tail = yield* files.head({ folderId });
    const stored = yield* Projection.read(folderHead.at({ folderId }));

    if (compare(stored.offsets.input, tail.nextOffset) < 0) lagging.push(folderId);
  }

  return lagging;
});

/**
 * The Worker's only writer of the `folderHead` checkpoint, as the follower is on
 * the Bun host. Reads and `decide` fold forward from this checkpoint in memory,
 * so they no longer fold the whole folder log. The wakeup permit stops two runs
 * from overlapping.
 */
export const headWakeup = Wakeup.make({
  name: "folder-sync/head",
  // A deadline in the past means "due now". The host arms the alarm at the next millisecond.
  next: behind.pipe(
    Effect.map((lagging) => (lagging.length === 0 ? Option.none() : Option.some(0))),
    Effect.mapError(fault),
  ),
  run: Effect.gen(function* () {
    let more = false;

    for (const folderId of yield* behind) {
      const progress = yield* Projection.run(folderHead.at({ folderId }), {
        limit: UNITS_PER_RUN,
      });

      if (progress.status === "limit-reached") more = true;
    }

    return { more };
  }),
});
