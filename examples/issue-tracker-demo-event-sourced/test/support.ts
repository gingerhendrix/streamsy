import { BunCrypto } from "@effect/platform-bun";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { Effect, Layer, Schedule, Schema } from "effect";
import type { Stamp } from "../server/decide.ts";
import { IssueTrackerLive } from "../server/issue-tracker.ts";
import { streams, Workspaces } from "../server/streams.ts";
import { ProjectionWatchersLive } from "../server/watchers.ts";

export const createdAt = "2026-10-06T00:00:00.000Z";

export const later = "2026-10-06T00:05:00.000Z";

export const stamp = (id: string, now = createdAt, txid = crypto.randomUUID()): Stamp => ({
  id,
  now,
  txid,
});

/**
 * A memory store with the checkpoint store on the same owner, as the server
 * builds it. Each `Effect.provide` builds a new, empty store.
 */
export const MemoryHost = CheckpointStore.layer.pipe(
  Layer.provideMerge(MemoryStorage.layer(streams)),
);

/** A Bun SQLite store in `filename`, as the server builds it with `ISSUE_TRACKER_DB`. */
export const fileHost = (filename: string) =>
  CheckpointStore.layer.pipe(
    Layer.provideMerge(BunStorage.layer(streams, { client: { filename } })),
  );

/** The tracker, its watchers and crypto over a host. The host stays visible to the test. */
export const trackerOn = <E>(host: Layer.Layer<Layer.Success<typeof MemoryHost>, E>) =>
  IssueTrackerLive.pipe(
    Layer.provideMerge(ProjectionWatchersLive),
    Layer.provideMerge(Layer.merge(host, BunCrypto.layer)),
  );

/** Every service that a tracker test can use. */
export type TrackerServices = Layer.Success<ReturnType<typeof trackerOn>>;

class NotYet extends Schema.TaggedError<NotYet>()("NotYet", {}) {}

/** The State changes of one workspace, read once. */
export const stateChanges = (workspaceId: string) =>
  Effect.map(
    Effect.flatMap(Workspaces, (rows) => rows.collect({ workspaceId })),
    ({ items }) => items,
  );

/**
 * Wait until the State stream of the workspace holds a change with each txid.
 * The watcher writes the rows after the command answers.
 */
export const awaitTxids = (workspaceId: string, ...txids: ReadonlyArray<string>) =>
  stateChanges(workspaceId).pipe(
    Effect.flatMap((items) =>
      txids.every((txid) => items.some((item) => item.headers.txid === txid))
        ? Effect.succeed(items)
        : Effect.fail(new NotYet()),
    ),
    Effect.retry({ while: Schema.is(NotYet), schedule: Schedule.spaced("10 millis"), times: 300 }),
  );
