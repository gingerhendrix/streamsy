import { checkFamilies, type AnyFamily, type Identifiers } from "./family.ts";

/** An ordered, immutable declaration shared by a host and its clients. */
export interface StreamGroup<Keys extends ReadonlyArray<AnyFamily> = ReadonlyArray<AnyFamily>> {
  readonly families: Keys;
}
export type Services<Group extends StreamGroup> = Identifiers<Group["families"]>;
export type Input<Keys extends ReadonlyArray<AnyFamily>> = Keys | readonly [StreamGroup<Keys>];

export function make<const Keys extends ReadonlyArray<AnyFamily>>(
  ...keys: Keys
): StreamGroup<Keys> {
  checkFamilies(keys);
  return Object.freeze({ families: Object.freeze(keys) });
}

/** Normalize the two public argument forms without losing their family tuple. */
export function families<const Keys extends ReadonlyArray<AnyFamily>>(input: Input<Keys>): Keys {
  const first = input[0];
  // SAFETY: Input is either the family tuple or one group containing that same tuple.
  return (first !== undefined && "families" in first ? first.families : input) as Keys;
}
