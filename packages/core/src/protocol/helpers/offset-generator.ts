/**
 * Offset allocation, validation, and comparison for the durable streams protocol.
 *
 * Responsibilities:
 *
 * - offsets are lexicographically ordered strings of the form
 *   `<counter:16>_<sub:16>` where both parts are zero-padded decimals;
 * - `isValidOffset` validates the public offset format;
 * - `compareOffsets` compares offsets by their lexicographic order;
 * - `formatCounter` and `parseCounter` convert between numeric counters and
 *   public offsets;
 * - `allocate` produces the next contiguous offsets for a mutation batch.
 *
 * Used by `StreamProtocol` mutation helpers, `ReadService`, `LiveReadService`,
 * and `ForkService` for allocation, cursor/tail comparisons, and fork-offset
 * validation.
 */

import type { Offset } from "../../types/storage.ts";

export const ZERO_OFFSET: Offset = `${"0".repeat(16)}_${"0".repeat(16)}`;

const OFFSET_REGEX = /^\d{1,16}_\d{1,16}$/;

export function isValidOffset(offset: string): boolean {
  return OFFSET_REGEX.test(offset);
}

export function compareOffsets(a: Offset, b: Offset): number {
  return a === b ? 0 : a < b ? -1 : 1;
}

export function formatCounter(counter: number): Offset {
  return `${String(counter).padStart(16, "0")}_${"0".repeat(16)}`;
}

export function parseCounter(offset: Offset): number {
  return parseInt(offset.split("_")[0] ?? "0", 10);
}

export interface OffsetAllocation {
  endCounter: number;
  nextOffset: Offset;
  offsets: Offset[];
}

export function allocate(counter: number, count: number): OffsetAllocation {
  const offsets: Offset[] = [];
  for (let i = 1; i <= count; i++) offsets.push(formatCounter(counter + i));
  const endCounter = counter + count;
  return {
    endCounter,
    offsets,
    nextOffset: count > 0 ? formatCounter(endCounter) : formatCounter(counter),
  };
}
