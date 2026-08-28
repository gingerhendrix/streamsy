import type { StorageDeleteResult, DeletePlan } from "../../types/storage-adapter.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";
import type { LineagePolicy, LineageStore } from "./lineage-store.ts";

export async function cascadeReclaim(
  store: LineageStore,
  plan: DeletePlan,
  lineage: LineagePolicy,
): Promise<StorageDeleteResult> {
  const record = await store.getRecord(plan.streamId);
  if (!record) return { status: "not-found" };
  if (plan.reason === "expiry" && record.lifecycle.expiresAtMs !== plan.expectedExpiresAtMs)
    return { status: "expiry-mismatch" };
  if (plan.reason === "delete" && record.lifecycle.softDeleted) return { status: "gone" };

  const dependents = await lineage.countDependents(plan.streamId);
  if (dependents > 0) {
    const softened = await store.softDelete(
      plan.streamId,
      plan.reason === "expiry" ? plan.expectedExpiresAtMs : undefined,
    );
    if (!softened) return { status: plan.reason === "expiry" ? "expiry-mismatch" : "not-found" };
    return { status: "retained-soft-deleted" };
  }

  const purged = await store.purgeSelf(
    record.id,
    plan.reason === "expiry" ? plan.expectedExpiresAtMs : undefined,
  );
  if (!purged) return { status: plan.reason === "expiry" ? "expiry-mismatch" : "not-found" };
  await cascadeParents(store, lineage, record);
  return { status: "purged" };
}

export async function plainPurge(
  store: LineageStore,
  plan: DeletePlan,
): Promise<StorageDeleteResult> {
  const record = await store.getRecord(plan.streamId);
  if (!record) return { status: "not-found" };
  if (plan.reason === "expiry" && record.lifecycle.expiresAtMs !== plan.expectedExpiresAtMs)
    return { status: "expiry-mismatch" };
  if (plan.reason === "delete" && record.lifecycle.softDeleted) return { status: "gone" };
  const purged = await store.purgeSelf(
    plan.streamId,
    plan.reason === "expiry" ? plan.expectedExpiresAtMs : undefined,
  );
  if (!purged) return { status: plan.reason === "expiry" ? "expiry-mismatch" : "not-found" };
  return { status: "purged" };
}

async function cascadeParents(
  store: LineageStore,
  lineage: LineagePolicy,
  record: StreamRecord,
): Promise<void> {
  let childId: StreamId = record.id;
  let parentId = record.lifecycle.forkedFrom;
  while (parentId) {
    await lineage.dropEdge(parentId, childId);
    const parent = await store.getRecord(parentId);
    if (!parent || parent.lifecycle.softDeleted !== true) return;
    const dependents = await lineage.countDependents(parentId);
    if (dependents > 0) return;

    await store.purgeSelf(parentId);
    childId = parentId;
    parentId = parent.lifecycle.forkedFrom;
  }
}
