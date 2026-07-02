/**
 * Native in-memory {@link StorageAdapter}.
 *
 * Owns a process-wide registry of private `MemoryStream` handles. The adapter is
 * flat: every per-stream method takes `streamId` and delegates to the handle that
 * `state.getStream(id)` owns; lifecycle intents reuse the handle's internal write
 * engine to insert records.
 */
import type {
  CreatePlan,
  DeletePlan,
  ForkPlan,
  StorageAdapter,
} from "../../types/storage-adapter.ts";
import type { StreamId } from "../../types/storage.ts";
import { compareOffsets } from "../../protocol/helpers/offset-generator.ts";
import { cascadeReclaim, refCountLineage } from "../strategies/index.ts";
import { MemoryLineageStore } from "./lineage-store.ts";
import { MemoryStreamState } from "./state.ts";

export interface MemoryStorageAdapterOptions {
  /**
   * Share an existing memory state. Omit to get a fresh, isolated state.
   */
  state?: MemoryStreamState;
  onScheduledExpiry?: (streamId: StreamId) => Promise<void> | void;
}

export function createMemoryStorageAdapter(
  options: MemoryStorageAdapterOptions = {},
): StorageAdapter {
  const state = options.state ?? new MemoryStreamState(options.onScheduledExpiry);
  const lineageStore = new MemoryLineageStore(state);
  const lineage = refCountLineage(lineageStore);
  return {
    getRecord: (streamId) => Promise.resolve(state.getStream(streamId).getRecord()),
    listMessages: (streamId, listOptions) =>
      Promise.resolve(state.getStream(streamId).listMessages(listOptions)),
    getProducerState: (streamId, producerId) =>
      Promise.resolve(state.getStream(streamId).getProducerState(producerId)),
    append: (streamId, plan) => Promise.resolve(state.getStream(streamId).append(plan)),
    awaitChange: (streamId, awaitOptions) => state.getStream(streamId).awaitChange(awaitOptions),
    scheduleExpiry: (streamId, at) => state.getStream(streamId).scheduleExpiry(at),
    cancelExpiry: (streamId) => state.getStream(streamId).cancelExpiry(),
    async create(plan: CreatePlan) {
      const stream = state.getStream(plan.record.id);
      // `plan.record` is the single source of truth — a created-closed stream
      // arrives with `lifecycle.closed`/`closedAt` already folded in by core.
      const result = stream.applyMutation({
        createRecord: plan.record,
        preconditions: {},
        messages: plan.initialMessages,
      });
      if (result.status === "committed") return { status: "created", record: result.record };
      return { status: "exists", record: result.record ?? plan.record };
    },
    async fork(plan: ForkPlan) {
      const source = state.getExistingStream(plan.sourceId)?.getRecord() ?? null;
      if (
        !source ||
        source.lifecycle.softDeleted ||
        compareOffsets(source.currentOffset, plan.precondition.sourceLiveAtOffset) < 0
      )
        return { status: "fork-source-gone" };

      const child = state.getStream(plan.child.id);
      const result = child.applyMutation({
        createRecord: plan.child,
        preconditions: {},
        messages: plan.initialMessages,
      });
      if (result.status !== "committed") {
        const existing = result.record ?? child.getRecord() ?? plan.child;
        // Convergent saga repair: an earlier fork that committed the child but
        // lost the lineage edge is healed by the idempotent re-add.
        if (existing.lifecycle.forkedFrom === plan.sourceId) {
          await lineage.addEdge(plan.sourceId, plan.child.id);
        }
        return { status: "exists", record: existing };
      }

      await lineage.addEdge(plan.sourceId, plan.child.id);
      return { status: "created", record: result.record };
    },
    delete(plan: DeletePlan) {
      return cascadeReclaim(lineageStore, plan, lineage);
    },
  };
}
