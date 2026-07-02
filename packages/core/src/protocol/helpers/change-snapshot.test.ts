import { describe, expect, it } from "vitest";
import type { StreamRecord } from "../../types/storage.ts";
import { buildChangeSnapshot, changeSnapshotDiffers } from "./change-snapshot.ts";
import { formatCounter, ZERO_OFFSET } from "./offset-generator.ts";

const OFFSET_1 = formatCounter(1);
const OFFSET_2 = formatCounter(2);

function record(over: Partial<StreamRecord> = {}): StreamRecord {
  return {
    id: "s",
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle: {},
    currentOffset: OFFSET_1,
    counter: 1,
    ...over,
  };
}

describe("buildChangeSnapshot", () => {
  it("reports an absent record as not present at the zero offset", () => {
    expect(buildChangeSnapshot(null)).toEqual({
      present: false,
      currentOffset: ZERO_OFFSET,
      closed: false,
      softDeleted: false,
    });
  });

  it("projects the change-relevant fields of a present record", () => {
    const snapshot = buildChangeSnapshot(
      record({ lifecycle: { closed: true, softDeleted: true }, currentOffset: OFFSET_2 }),
    );
    expect(snapshot).toEqual({
      present: true,
      currentOffset: OFFSET_2,
      closed: true,
      softDeleted: true,
    });
  });

  it("treats absent lifecycle flags as false", () => {
    const snapshot = buildChangeSnapshot(record());
    expect(snapshot.closed).toBe(false);
    expect(snapshot.softDeleted).toBe(false);
  });
});

describe("changeSnapshotDiffers", () => {
  const observed = { fromOffset: OFFSET_1, observedClosed: false, observedSoftDeleted: false };

  it("differs when the record is purged", () => {
    expect(changeSnapshotDiffers(buildChangeSnapshot(null), observed)).toBe(true);
  });

  it("differs when the offset advanced past the parked position", () => {
    const snapshot = buildChangeSnapshot(record({ currentOffset: OFFSET_2 }));
    expect(changeSnapshotDiffers(snapshot, observed)).toBe(true);
  });

  it("does not differ when the offset is unchanged", () => {
    expect(changeSnapshotDiffers(buildChangeSnapshot(record()), observed)).toBe(false);
  });

  it("differs when the offset regressed below the parked position (purge → re-create)", () => {
    // Within one incarnation offsets only advance, so a lower offset means the
    // stream was purged and re-created while the waiter was parked.
    const snapshot = buildChangeSnapshot(record({ currentOffset: ZERO_OFFSET }));
    expect(changeSnapshotDiffers(snapshot, observed)).toBe(true);
  });

  it("differs when closed transitions to true the caller had not observed", () => {
    const snapshot = buildChangeSnapshot(record({ lifecycle: { closed: true } }));
    expect(changeSnapshotDiffers(snapshot, observed)).toBe(true);
  });

  it("does not differ when the caller already observed closed", () => {
    const snapshot = buildChangeSnapshot(record({ lifecycle: { closed: true } }));
    expect(changeSnapshotDiffers(snapshot, { ...observed, observedClosed: true })).toBe(false);
  });

  it("differs when soft-deleted transitions to true the caller had not observed", () => {
    const snapshot = buildChangeSnapshot(record({ lifecycle: { softDeleted: true } }));
    expect(changeSnapshotDiffers(snapshot, observed)).toBe(true);
  });

  it("does not differ when the caller already observed soft-deleted", () => {
    const snapshot = buildChangeSnapshot(record({ lifecycle: { softDeleted: true } }));
    expect(changeSnapshotDiffers(snapshot, { ...observed, observedSoftDeleted: true })).toBe(false);
  });

  it("treats absent observed flags as false (a fresh terminal transition differs)", () => {
    const snapshot = buildChangeSnapshot(record({ lifecycle: { closed: true } }));
    expect(changeSnapshotDiffers(snapshot, { fromOffset: OFFSET_1 })).toBe(true);
  });
});
