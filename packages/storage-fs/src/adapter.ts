/**
 * JSONL filesystem {@link StorageAdapter}.
 *
 * Persists each stream as a directory under `root` (record.json + messages.jsonl
 * + producers.json + .lock) and owns a per-id cache of private `FsStream`
 * handles. The adapter is flat: every per-stream method takes `streamId` and
 * delegates to `state.getStream(id)`; lifecycle intents carry the id in a plan.
 *
 * **`awaitChange` (required).** Implemented via core's exported
 * `runAwaitChangeLoop` with every park capped at `watchPollMs` (`parkCapMs`).
 * The in-process notifier is always a wake source; enabling `watch` races in
 * `fs.watch` as the cross-process wake source (the HTTP-frontend +
 * serverless-writers use-case). With watch off, a cross-process write is
 * observed on the next capped re-read — the toggle changes wake latency,
 * never the capability.
 *
 * **Fork.** v1 is intentionally forkless (`fork` omitted): none of the target
 * use-cases need it, the conformance kit skips fork-dependent cases for a forkless
 * adapter, and `delete` reduces to a plain purge (no dependents possible). The
 * on-disk layout already records `lifecycle.forkedFrom`, so fork can be added
 * later (reusing core lineage strategies) without a format change.
 */
import type { CreatePlan, DeletePlan, StorageAdapter } from "@streamsy/core";
import { FsStreamState, type FsStreamStateOptions } from "./state.ts";

export interface FsStorageAdapterOptions extends FsStreamStateOptions {
  /** Share an existing state instead of constructing one from `root`. */
  state?: FsStreamState;
}

export interface FsStorageAdapter extends StorageAdapter {
  readonly state: FsStreamState;
}

export function createFsStorageAdapter(options: FsStorageAdapterOptions): FsStorageAdapter {
  const { state: existing, ...stateOptions } = options;
  const state = existing ?? new FsStreamState(stateOptions);

  return {
    state,
    getRecord: (streamId) => state.getStream(streamId).getRecord(),
    listMessages: (streamId, listOptions) => state.getStream(streamId).listMessages(listOptions),
    getProducerState: (streamId, producerId) =>
      state.getStream(streamId).getProducerState(producerId),
    append: (streamId, plan) => state.getStream(streamId).append(plan),
    awaitChange: (streamId, awaitOptions) => state.getStream(streamId).awaitChange(awaitOptions),
    scheduleExpiry: (streamId, at) => state.getStream(streamId).scheduleExpiry(at),
    cancelExpiry: (streamId) => state.getStream(streamId).cancelExpiry(),
    async create(plan: CreatePlan) {
      const stream = state.getStream(plan.record.id);
      // `plan.record` is the single source of truth — a created-closed stream
      // arrives with `lifecycle.closed`/`closedAt` already folded in by core.
      const result = await stream.applyMutation({
        createRecord: plan.record,
        preconditions: {},
        messages: plan.initialMessages,
      });
      if (result.status === "committed") return { status: "created", record: result.record };
      return { status: "exists", record: result.record ?? plan.record };
    },
    delete(plan: DeletePlan) {
      return state.getStream(plan.streamId).remove(plan.reason);
    },
  };
}
