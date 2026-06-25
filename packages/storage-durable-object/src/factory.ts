/**
 * Native Durable Object `StreamFactory`.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id is
 * routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`. The Durable Object owns persistent stream
 * state and exposes commit/read/runtime RPC methods; this module returns the
 * small protocol-facing proxy needed for local-only `Stream` concerns (`id`)
 * that do not map cleanly to Cloudflare RPC.
 */
import type {
  CreatePlan,
  DeletePlan,
  ForkPlan,
  Stream,
  StreamFactory,
  StreamId,
} from "@streamsy/core";
import { cascadeReclaim, refCountLineage } from "@streamsy/core";
import { DurableObjectLineageStore } from "./lineage-store.ts";
import { DurableObjectStreamProxy } from "./proxy.ts";
import type { DurableObjectStreamStorage } from "./storage.ts";

export interface DurableObjectStreamFactoryOptions {
  namespace: DurableObjectNamespace<DurableObjectStreamStorage>;
}

export function createDurableObjectStreamFactory(
  options: DurableObjectStreamFactoryOptions,
): StreamFactory {
  const { namespace } = options;
  const lineageStore = new DurableObjectLineageStore(namespace);
  const lineage = refCountLineage(lineageStore);

  async function getStream(streamId: StreamId): Promise<DurableObjectStreamProxy> {
    const stub = namespace.get(namespace.idFromName(streamId));
    await stub.init(streamId);
    return new DurableObjectStreamProxy(streamId, stub);
  }

  return {
    async getStream(streamId: StreamId): Promise<Stream> {
      return getStream(streamId);
    },
    async create(plan: CreatePlan) {
      const stream = await getStream(plan.record.id);
      const closedAt = plan.record.lifecycle.closedAt;
      const result = await stream.commit({
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
      const source = await (await getStream(plan.sourceId)).getRecord();
      if (
        !source ||
        source.lifecycle.softDeleted ||
        // Durable Streams offsets are protocol-defined fixed-width sortable strings;
        // lexical comparison is therefore the required source-liveness check.
        source.currentOffset < plan.precondition.sourceLiveAtOffset
      )
        return { status: "fork-source-gone" };

      const child = await getStream(plan.child.id);
      const result = await child.commit({
        createRecord: plan.child,
        preconditions: {},
        appendMessages: plan.initialMessages,
      });

      if (result.status === "committed") {
        await lineage.addEdge(plan.sourceId, plan.child.id);
        return { status: "created", record: result.record };
      }

      const existingChild = await child.getRecord();
      if (existingChild?.lifecycle.forkedFrom === plan.sourceId) {
        await lineage.addEdge(plan.sourceId, plan.child.id);
      }
      return { status: "exists" };
    },
    delete(plan: DeletePlan) {
      return cascadeReclaim(lineageStore, plan, lineage);
    },
  };
}
