import type { DeleteCommit, DeletePlan } from "../../types/factory.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";
import type { LineagePolicy, LineageStore } from "./lineage-store.ts";

export async function cascadeReclaim(
  store: LineageStore,
  plan: DeletePlan,
  lineage: LineagePolicy,
): Promise<DeleteCommit> {
  const record = await store.getRecord(plan.streamId);
  if (!record) return { status: "not-found" };
  if (plan.reason === "delete" && record.lifecycle.softDeleted) return { status: "gone" };

  const dependents = await lineage.countDependents(plan.streamId);
  if (dependents > 0) {
    await store.softDelete(plan.streamId);
    return { status: "retained-soft-deleted" };
  }

  await purgeAndCascade(store, lineage, record);
  return { status: "purged" };
}

export async function plainPurge(store: LineageStore, plan: DeletePlan): Promise<DeleteCommit> {
  const record = await store.getRecord(plan.streamId);
  if (!record) return { status: "not-found" };
  if (plan.reason === "delete" && record.lifecycle.softDeleted) return { status: "gone" };
  await store.purgeSelf(plan.streamId);
  return { status: "purged" };
}

async function purgeAndCascade(
  store: LineageStore,
  lineage: LineagePolicy,
  record: StreamRecord,
): Promise<void> {
  await store.purgeSelf(record.id);

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
