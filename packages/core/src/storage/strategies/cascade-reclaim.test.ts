import { describe, expect, it } from "vitest";
import type { DeletePlan } from "../../types/factory.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";
import { cascadeReclaim } from "./cascade-reclaim.ts";
import { refCountLineage } from "./ref-count-lineage.ts";
import { reverseIndexLineage } from "./reverse-index-lineage.ts";
import type { LineageStore } from "./lineage-store.ts";

function record(id: StreamId, forkedFrom?: StreamId, softDeleted = false): StreamRecord {
  return {
    id,
    config: { contentType: "application/octet-stream", createdAt: 1 },
    lifecycle: { forkedFrom, softDeleted },
    currentOffset: "0",
    counter: 0,
  };
}

function deletePlan(streamId: StreamId, reason: DeletePlan["reason"] = "delete"): DeletePlan {
  return { streamId, reason };
}

class FakeLineageStore implements LineageStore {
  readonly records = new Map<StreamId, StreamRecord>();
  readonly edges = new Map<StreamId, Set<StreamId>>();
  readonly purged: StreamId[] = [];
  readonly softened: StreamId[] = [];

  async getRecord(id: StreamId): Promise<StreamRecord | null> {
    return this.records.get(id) ?? null;
  }

  async purgeSelf(id: StreamId): Promise<void> {
    this.purged.push(id);
    this.records.delete(id);
  }

  async softDelete(id: StreamId): Promise<void> {
    this.softened.push(id);
    const current = this.records.get(id);
    if (current)
      this.records.set(id, { ...current, lifecycle: { ...current.lifecycle, softDeleted: true } });
  }

  async addEdge(parent: StreamId, child: StreamId): Promise<void> {
    const children = this.edges.get(parent) ?? new Set<StreamId>();
    children.add(child);
    this.edges.set(parent, children);
  }

  async dropEdge(parent: StreamId, child: StreamId): Promise<void> {
    this.edges.get(parent)?.delete(child);
  }

  async countDependents(parent: StreamId): Promise<number> {
    return this.edges.get(parent)?.size ?? 0;
  }
}

describe("cascadeReclaim", () => {
  it("soft-deletes a stream that still has dependents", async () => {
    const store = new FakeLineageStore();
    store.records.set("parent", record("parent"));
    await store.addEdge("parent", "child");

    const result = await cascadeReclaim(store, deletePlan("parent"), refCountLineage(store));

    expect(result).toEqual({ status: "retained-soft-deleted" });
    expect(store.softened).toEqual(["parent"]);
    expect(store.purged).toEqual([]);
    expect(store.records.get("parent")?.lifecycle.softDeleted).toBe(true);
  });

  it("purges a stream with no dependents", async () => {
    const store = new FakeLineageStore();
    store.records.set("stream", record("stream"));

    const result = await cascadeReclaim(store, deletePlan("stream"), refCountLineage(store));

    expect(result).toEqual({ status: "purged" });
    expect(store.purged).toEqual(["stream"]);
    expect(store.records.has("stream")).toBe(false);
  });

  it("drops the child edge and purges a soft-deleted parent once it has no dependents", async () => {
    const store = new FakeLineageStore();
    store.records.set("parent", record("parent", undefined, true));
    store.records.set("child", record("child", "parent"));
    await store.addEdge("parent", "child");

    const result = await cascadeReclaim(store, deletePlan("child"), refCountLineage(store));

    expect(result).toEqual({ status: "purged" });
    expect(store.purged).toEqual(["child", "parent"]);
    expect(await store.countDependents("parent")).toBe(0);
  });
});

describe("lineage policies", () => {
  it("refCountLineage delegates idempotent edge operations to the store", async () => {
    const store = new FakeLineageStore();
    const lineage = refCountLineage(store);

    await lineage.addEdge("parent", "child");
    await lineage.addEdge("parent", "child");

    expect(await lineage.countDependents("parent")).toBe(1);

    await lineage.dropEdge("parent", "child");

    expect(await lineage.countDependents("parent")).toBe(0);
  });

  it("reverseIndexLineage uses the injected dependents query and no-ops edge writes", async () => {
    const seen: StreamId[] = [];
    const lineage = reverseIndexLineage(async (parent) => {
      seen.push(parent);
      return parent === "parent" ? 2 : 0;
    });

    await lineage.addEdge("parent", "child");
    await lineage.dropEdge("parent", "child");

    expect(await lineage.countDependents("parent")).toBe(2);
    expect(seen).toEqual(["parent"]);
  });
});
