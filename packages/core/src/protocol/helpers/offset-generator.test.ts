/**
 * Unit coverage for the offset-generator helper extracted from
 * StreamProtocol. These tests pin the wire format and the lexicographic
 * ordering invariants used for fork-offset bounds and read-tail comparisons.
 */

import { describe, it, expect } from "vitest";
import {
  ZERO_OFFSET,
  allocate,
  compareOffsets,
  formatCounter,
  isValidOffset,
  parseCounter,
} from "../../protocol/helpers/offset-generator.ts";

describe("ZERO_OFFSET", () => {
  it("is 16 zero counter digits, underscore, 16 zero sub digits", () => {
    expect(ZERO_OFFSET).toBe(`${"0".repeat(16)}_${"0".repeat(16)}`);
    expect(ZERO_OFFSET).toMatch(/^0{16}_0{16}$/);
  });

  it("is recognised as a valid offset", () => {
    expect(isValidOffset(ZERO_OFFSET)).toBe(true);
  });
});

describe("isValidOffset", () => {
  it("accepts the zero offset and 1..16 digit halves", () => {
    expect(isValidOffset(ZERO_OFFSET)).toBe(true);
    expect(isValidOffset("1_1")).toBe(true);
    expect(isValidOffset(`${"9".repeat(16)}_${"9".repeat(16)}`)).toBe(true);
  });

  it("rejects malformed inputs", () => {
    expect(isValidOffset("")).toBe(false);
    expect(isValidOffset("0")).toBe(false);
    expect(isValidOffset("0_")).toBe(false);
    expect(isValidOffset("_0")).toBe(false);
    expect(isValidOffset("00000000000000000_0")).toBe(false); // 17-digit left half
    expect(isValidOffset("0_00000000000000000")).toBe(false); // 17-digit right half
    expect(isValidOffset("0_0_0")).toBe(false);
    expect(isValidOffset("a_0")).toBe(false);
    expect(isValidOffset("-1_0")).toBe(false);
  });
});

describe("compareOffsets", () => {
  it("returns 0 for equal offsets", () => {
    expect(compareOffsets(ZERO_OFFSET, ZERO_OFFSET)).toBe(0);
    expect(compareOffsets(formatCounter(7), formatCounter(7))).toBe(0);
  });

  it("orders by lexicographic comparison, which is numeric for fixed-width offsets", () => {
    expect(compareOffsets(formatCounter(0), formatCounter(1))).toBe(-1);
    expect(compareOffsets(formatCounter(2), formatCounter(1))).toBe(1);
    expect(compareOffsets(formatCounter(9), formatCounter(10))).toBe(-1);
    expect(compareOffsets(formatCounter(99), formatCounter(100))).toBe(-1);
  });
});

describe("formatCounter / parseCounter", () => {
  it("round-trips small and large counters", () => {
    for (const n of [0, 1, 9, 10, 99, 100, 1234567890, Number.MAX_SAFE_INTEGER]) {
      expect(parseCounter(formatCounter(n))).toBe(n);
    }
  });

  it("zero-pads counters to 16 digits", () => {
    expect(formatCounter(0)).toBe(ZERO_OFFSET);
    expect(formatCounter(1)).toBe(`${"0".repeat(15)}1_${"0".repeat(16)}`);
    expect(formatCounter(42)).toBe(`${"0".repeat(14)}42_${"0".repeat(16)}`);
  });

  it("formatCounter output passes isValidOffset", () => {
    for (const n of [0, 1, 999, 10 ** 12]) {
      expect(isValidOffset(formatCounter(n))).toBe(true);
    }
  });
});

describe("allocate", () => {
  it("returns the formatted current counter as nextOffset when count is 0", () => {
    const result = allocate(0, 0);
    expect(result.offsets).toEqual([]);
    expect(result.endCounter).toBe(0);
    expect(result.nextOffset).toBe(ZERO_OFFSET);

    const fromMid = allocate(5, 0);
    expect(fromMid.offsets).toEqual([]);
    expect(fromMid.endCounter).toBe(5);
    expect(fromMid.nextOffset).toBe(formatCounter(5));
  });

  it("allocates `count` strictly increasing offsets starting at counter+1", () => {
    const result = allocate(3, 4);
    expect(result.offsets).toEqual([
      formatCounter(4),
      formatCounter(5),
      formatCounter(6),
      formatCounter(7),
    ]);
    expect(result.endCounter).toBe(7);
    expect(result.nextOffset).toBe(formatCounter(7));
  });

  it("nextOffset on a non-empty allocation equals the last allocated offset", () => {
    const result = allocate(0, 3);
    expect(result.offsets).toHaveLength(3);
    expect(result.nextOffset).toBe(result.offsets[result.offsets.length - 1]);
  });

  it("successive allocations chain via endCounter without gaps or overlap", () => {
    const first = allocate(0, 3);
    const second = allocate(first.endCounter, 2);
    expect(second.offsets[0]).toBe(formatCounter(4));
    expect(second.endCounter).toBe(5);
    const allOffsets = [...first.offsets, ...second.offsets];
    for (let i = 1; i < allOffsets.length; i++) {
      expect(compareOffsets(allOffsets[i]!, allOffsets[i - 1]!)).toBe(1);
    }
  });
});
