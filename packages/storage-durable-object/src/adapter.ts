/**
 * Native Durable Object {@link StorageAdapter}.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id is
 * routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`. The adapter is flat — every method routes to
 * the bound stub, passing `streamId` so the DO self-initializes on first access.
 * No forwarding proxy and no separate `init` round-trip.
 */
import type { CreatePlan, DeletePlan, ForkPlan, StorageAdapter, StreamId } from "@streamsy/core";
import { cascadeReclaim, refCountLineage } from "@streamsy/core";
import { DurableObjectLineageStore } from "./lineage-store.ts";
import type { DurableObjectStreamStorage } from "./storage.ts";

type DurableObjectStreamStub = DurableObjectStub<DurableObjectStreamStorage>;

export interface DurableObjectStorageAdapterOptions {
  namespace: DurableObjectNamespace<DurableObjectStreamStorage>;
}

export function createDurableObjectStorageAdapter(
  options: DurableObjectStorageAdapterOptions,
): StorageAdapter {
  const { namespace } = options;
  const lineageStore = new DurableObjectLineageStore(namespace);
  const lineage = refCountLineage(lineageStore);

  const stub = (streamId: StreamId): DurableObjectStreamStub =>
    namespace.get(namespace.idFromName(streamId));

  return {
    getRecord: (streamId) => stub(streamId).getRecord(streamId),
    listMessages: (streamId, listOptions) => stub(streamId).listMessages(streamId, listOptions),
    getProducerState: (streamId, producerId) =>
      stub(streamId).getProducerState(streamId, producerId),
    append: (streamId, plan) => stub(streamId).append(streamId, plan),
    awaitChange: (streamId, awaitOptions) => stub(streamId).awaitChange(streamId, awaitOptions),
    scheduleExpiry: (streamId, at) => stub(streamId).scheduleExpiry(streamId, at),
    cancelExpiry: (streamId) => stub(streamId).cancelExpiry(streamId),
    async create(plan: CreatePlan) {
      // `plan.record` is the single source of truth — a created-closed stream
      // arrives with `lifecycle.closed`/`closedAt` already folded in by core.
      const result = await stub(plan.record.id).applyMutation(plan.record.id, {
        createRecord: plan.record,
        preconditions: {},
        messages: plan.initialMessages,
      });
      if (result.status === "committed") return { status: "created", record: result.record };
      return { status: "exists", record: result.record ?? plan.record };
    },
    async fork(plan: ForkPlan) {
      const source = await stub(plan.sourceId).getRecord(plan.sourceId);
      if (
        !source ||
        source.lifecycle.softDeleted ||
        // Durable Streams offsets are protocol-defined fixed-width sortable strings;
        // lexical comparison is therefore the required source-liveness check.
        source.currentOffset < plan.precondition.sourceLiveAtOffset
      )
        return { status: "fork-source-gone" };

      const result = await stub(plan.child.id).applyMutation(plan.child.id, {
        createRecord: plan.child,
        preconditions: {},
        messages: plan.initialMessages,
      });

      if (result.status === "committed") {
        await lineage.addEdge(plan.sourceId, plan.child.id);
        return { status: "created", record: result.record };
      }

      const existingChild = await stub(plan.child.id).getRecord(plan.child.id);
      if (existingChild?.lifecycle.forkedFrom === plan.sourceId) {
        // Convergent saga repair: heal a lineage edge lost between an earlier
        // fork's child commit and its edge add.
        await lineage.addEdge(plan.sourceId, plan.child.id);
      }
      // `exists` carries the existing child so core can run its config-match
      // idempotency (identical racing forks are not conflicts).
      return { status: "exists", record: existingChild ?? plan.child };
    },
    delete(plan: DeletePlan) {
      return cascadeReclaim(lineageStore, plan, lineage);
    },
  };
}
