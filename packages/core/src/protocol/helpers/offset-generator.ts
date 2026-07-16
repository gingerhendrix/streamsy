/** Offset generation and lexical ordering for the durable streams protocol. */

import type { Offset } from "../../types/storage.ts";

/**
 * Generates opaque offsets for one stream. `next` is called once per message,
 * including every item in a JSON batch. Core verifies every returned value is
 * valid and strictly greater than `previous` before a storage plan is built.
 */
export interface OffsetGenerator {
  /** Tail value for a newly-created empty stream. */
  readonly initialOffset: Offset;
  /** Return the next offset after `previous`. */
  next(previous: Offset): Offset;
  /** Accept client-supplied lexical boundaries produced by this scheme. */
  isValid(offset: string): boolean;
}

export const ZERO_OFFSET: Offset = `${"0".repeat(16)}_${"0".repeat(16)}`;
const FIXED_WIDTH_OFFSET_REGEX = /^\d{16}_\d{16}$/;
const FORBIDDEN_OFFSET_CHARACTERS = /[,&=?/]/;

/** Protocol-level constraints that apply to every generated offset scheme. */
export function isSafeOffsetToken(offset: string): boolean {
  return (
    offset.length > 0 &&
    offset.length < 256 &&
    offset !== "-1" &&
    offset !== "now" &&
    !FORBIDDEN_OFFSET_CHARACTERS.test(offset)
  );
}

export function isValidOffset(generator: OffsetGenerator, offset: string): boolean {
  return isSafeOffsetToken(offset) && generator.isValid(offset);
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

/** The backwards-compatible fixed-width counter generator used by default. */
export const defaultOffsetGenerator: OffsetGenerator = Object.freeze({
  initialOffset: ZERO_OFFSET,
  isValid(offset: string): boolean {
    return FIXED_WIDTH_OFFSET_REGEX.test(offset);
  },
  next(previous: Offset): Offset {
    return formatCounter(parseCounter(previous) + 1);
  },
});

export interface OffsetAllocation {
  endCounter: number;
  nextOffset: Offset;
  offsets: Offset[];
}

export class InvalidGeneratedOffsetError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidGeneratedOffsetError";
  }
}

/** Allocate a batch without interpreting the generator's opaque offset format. */
export function allocate(
  generator: OffsetGenerator,
  previousOffset: Offset,
  counter: number,
  count: number,
): OffsetAllocation {
  const offsets: Offset[] = [];
  let previous = previousOffset;
  for (let i = 0; i < count; i++) {
    const next = generator.next(previous);
    if (!isValidOffset(generator, next)) {
      throw new InvalidGeneratedOffsetError(`Offset generator returned an invalid token: ${next}`);
    }
    if (compareOffsets(next, previous) <= 0) {
      throw new InvalidGeneratedOffsetError(
        `Offset generator did not advance lexicographically: ${previous} -> ${next}`,
      );
    }
    offsets.push(next);
    previous = next;
  }
  return {
    endCounter: counter + count,
    offsets,
    nextOffset: previous,
  };
}

export function assertValidOffsetGenerator(generator: OffsetGenerator): void {
  if (!isValidOffset(generator, generator.initialOffset)) {
    throw new InvalidGeneratedOffsetError("Offset generator has an invalid initialOffset");
  }
}
