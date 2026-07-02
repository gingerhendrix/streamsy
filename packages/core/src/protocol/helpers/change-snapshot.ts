/**
 * Shared building blocks for the level-triggered `awaitChange` contract.
 *
 * Every adapter `awaitChange` implementation — whether it wakes natively or
 * polls its own durable reads — must decide "did change-relevant state advance
 * past what the caller observed?" identically. Centralizing the snapshot
 * construction and the diff here keeps that contract in one place.
 */
import type {
  AwaitChangeOptions,
  StreamChangeSnapshot,
  StreamRecord,
} from "../../types/storage.ts";
import { compareOffsets, ZERO_OFFSET } from "./offset-generator.ts";

/** Build the serializable, change-relevant snapshot of a (possibly absent) record. */
export function buildChangeSnapshot(record: StreamRecord | null): StreamChangeSnapshot {
  if (!record)
    return { present: false, currentOffset: ZERO_OFFSET, closed: false, softDeleted: false };
  return {
    present: true,
    currentOffset: record.currentOffset,
    closed: record.lifecycle.closed === true,
    softDeleted: record.lifecycle.softDeleted === true,
  };
}

/**
 * The level-triggered "differs" predicate. A snapshot differs from what the
 * caller observed when the record was purged, transitioned to soft-deleted or
 * closed, or its offset is not the parked position. Offset **inequality** — not
 * just advance — is the trigger: within one incarnation an offset only ever
 * advances, so a *lower* offset means the stream was purged and re-created
 * while the waiter was parked, which is also "something happened".
 */
export function changeSnapshotDiffers(
  snapshot: StreamChangeSnapshot,
  options: Pick<AwaitChangeOptions, "fromOffset" | "observedClosed" | "observedSoftDeleted">,
): boolean {
  if (!snapshot.present) return true;
  if (snapshot.softDeleted && options.observedSoftDeleted !== true) return true;
  if (snapshot.closed && options.observedClosed !== true) return true;
  return compareOffsets(snapshot.currentOffset, options.fromOffset) !== 0;
}
