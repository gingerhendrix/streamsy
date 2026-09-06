import { Offset } from "../schema/index.ts";
export const ZERO_OFFSET = Offset.make(`${"0".repeat(16)}_${"0".repeat(16)}`);
export const isValid = (offset: string): boolean => /^\d{16}_\d{16}$/.test(offset);
export const compare = (a: string, b: string): number => (a === b ? 0 : a < b ? -1 : 1);
const parse = (offset: Offset): bigint => BigInt(offset.slice(0, 16));
/** Fixed-width successor; bigint preserves all sixteen decimal digits. */
export function next(previous: Offset): Offset {
  const value = parse(previous) + 1n;
  if (value > 9999999999999999n) throw new RangeError("Offset exhausted");
  return Offset.make(`${String(value).padStart(16, "0")}_${"0".repeat(16)}`);
}
