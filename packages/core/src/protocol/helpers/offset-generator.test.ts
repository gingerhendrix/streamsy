import { describe, expect, it } from "vitest";
import {
  InvalidGeneratedOffsetError,
  ZERO_OFFSET,
  allocate,
  compareOffsets,
  defaultOffsetGenerator,
  formatCounter,
  isSafeOffsetToken,
  isValidOffset,
  parseCounter,
  type OffsetGenerator,
} from "./offset-generator.ts";

describe("defaultOffsetGenerator", () => {
  it("preserves the fixed-width wire format", () => {
    expect(defaultOffsetGenerator.initialOffset).toBe(`${"0".repeat(16)}_${"0".repeat(16)}`);
    expect(defaultOffsetGenerator.next(ZERO_OFFSET)).toBe(formatCounter(1));
    expect(defaultOffsetGenerator.next(formatCounter(41))).toBe(formatCounter(42));
  });

  it("accepts only canonical fixed-width boundaries", () => {
    expect(isValidOffset(defaultOffsetGenerator, ZERO_OFFSET)).toBe(true);
    expect(isValidOffset(defaultOffsetGenerator, formatCounter(1))).toBe(true);
    expect(isValidOffset(defaultOffsetGenerator, "1_0")).toBe(false);
    expect(isValidOffset(defaultOffsetGenerator, "0000000000000001_0")).toBe(false);
  });
});

describe("opaque offset allocation", () => {
  const custom: OffsetGenerator = {
    initialOffset: "00000000000000000000000000",
    isValid: (offset) => /^[0-9A-Z]{26}$/.test(offset),
    next: (previous) =>
      `${previous.slice(0, -1)}${String.fromCharCode(previous.charCodeAt(25) + 1)}`,
  };

  it("allocates a batch by chaining generated strings, without numeric parsing", () => {
    const result = allocate(custom, custom.initialOffset, 7, 3);
    expect(result.offsets).toEqual([
      "00000000000000000000000001",
      "00000000000000000000000002",
      "00000000000000000000000003",
    ]);
    expect(result.nextOffset).toBe("00000000000000000000000003");
    expect(result.endCounter).toBe(10);
  });

  it("keeps the previous tail for an empty mutation", () => {
    expect(allocate(custom, "00000000000000000000000003", 3, 0)).toEqual({
      offsets: [],
      nextOffset: "00000000000000000000000003",
      endCounter: 3,
    });
  });

  it("rejects duplicate, decreasing, reserved, and delimiter-bearing output", () => {
    for (const next of ["00000000000000000000000000", "-1", "now", "bad/value"]) {
      const broken: OffsetGenerator = {
        initialOffset: custom.initialOffset,
        isValid: () => true,
        next: () => next,
      };
      expect(() => allocate(broken, broken.initialOffset, 0, 1)).toThrow(
        InvalidGeneratedOffsetError,
      );
    }
  });
});

describe("shared helpers", () => {
  it("compares ordinary strings lexicographically", () => {
    expect(compareOffsets("01", "02")).toBe(-1);
    expect(compareOffsets("B", "A")).toBe(1);
    expect(compareOffsets("same", "same")).toBe(0);
  });

  it("retains fixed-width formatting helpers for compatibility", () => {
    for (const n of [0, 1, 9, 10, 99, 100, 1234567890, Number.MAX_SAFE_INTEGER]) {
      expect(parseCounter(formatCounter(n))).toBe(n);
    }
  });

  it("enforces protocol token constraints independent of generator format", () => {
    expect(isSafeOffsetToken("01HZX9Q9M3Z7ZJ8W4G5A6B7C8D")).toBe(true);
    for (const invalid of ["", "-1", "now", "a,b", "a&b", "a=b", "a?b", "a/b"]) {
      expect(isSafeOffsetToken(invalid)).toBe(false);
    }
  });
});
