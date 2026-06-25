/**
 * Native memory `StreamFactory`.
 *
 * Owns a process-wide registry of `MemoryStream` instances. Each lookup returns
 * the protocol-facing stream object for one id; `MemoryStream` implements the
 * core `Stream` interface directly and delegates storage/runtime operations to
 * simple bound stores.
 */
import type { Stream, StreamFactory } from "../../types/factory.ts";
import type { CreatePlan, DeletePlan, ForkPlan } from "../../types/factory.ts";
import type { StreamId } from "../../types/storage.ts";
import { compareOffsets } from "../../protocol/helpers/offset-generator.ts";
import { cascadeReclaim, refCountLineage } from "../strategies/index.ts";
import { MemoryLineageStore } from "./lineage-store.ts";
import { MemoryStreamState } from "./state.ts";

export interface MemoryStreamFactoryOptions {
  /**
   * Share an existing memory state. Omit to get a fresh, isolated state.
   */
  state?: MemoryStreamState;
  onScheduledExpiry?: (streamId: StreamId) => Promise<void> | void;
}

export function createMemoryStreamFactory(options: MemoryStreamFactoryOptions = {}): StreamFactory {
  const state = options.state ?? new MemoryStreamState(options.onScheduledExpiry);
  const lineageStore = new MemoryLineageStore(state);
  const lineage = refCountLineage(lineageStore);
  return {
    async getStream(streamId: StreamId): Promise<Stream> {
      return state.getStream(streamId);
    },
    async create(plan: CreatePlan) {
      const stream = state.getStream(plan.record.id);
      const closedAt = plan.record.lifecycle.closedAt;
      const result = stream.commitSync({
        createRecord: plan.record,
        preconditions: {},
        appendMessages: plan.initialMessages,
        recordPatch: plan.closeAfter
          ? { lifecycle: { closed: true, ...(closedAt !== undefined ? { closedAt } : {}) } }
          : undefined,
      });
      if (result.status === "committed") return { status: "created", record: result.record };
      return { status: "exists", record: result.record ?? plan.record };
    },
    async fork(plan: ForkPlan) {
      const source = state.getExistingStream(plan.sourceId)?.getRecordSync() ?? null;
      if (
        !source ||
        source.lifecycle.softDeleted ||
        compareOffsets(source.currentOffset, plan.precondition.sourceLiveAtOffset) < 0
      )
        return { status: "fork-source-gone" };

      const child = state.getStream(plan.child.id);
      const result = child.commitSync({
        createRecord: plan.child,
        preconditions: {},
        appendMessages: plan.initialMessages,
      });
      if (result.status !== "committed") return { status: "exists" };

      void lineage.addEdge(plan.sourceId, plan.child.id);
      return { status: "created", record: result.record };
    },
    delete(plan: DeletePlan) {
      return cascadeReclaim(lineageStore, plan, lineage);
    },
  };
}
