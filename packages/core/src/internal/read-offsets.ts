import type { ReadResult } from "../protocol/results.ts";

const Metadata = Symbol.for("@streamsy/core/internal/ReadOffsets");

export const attachOffsets = <A extends object>(result: A, offsets: ReadonlyArray<string>): A =>
  Object.defineProperty(result, Metadata, { value: offsets, enumerable: false });

export const offsetsOf = (
  result: Pick<ReadResult, "messages">,
): ReadonlyArray<string> | undefined => {
  if (!(Metadata in result)) return undefined;

  // SAFETY: only attachOffsets writes this private read metadata.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: attachOffsets writes storage offsets under the shared private symbol.
  return result[Metadata] as ReadonlyArray<string>;
};
