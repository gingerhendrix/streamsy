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

/** Family overloads require an argument; an explicit empty group is still valid. */
export type NonEmptyInput<Keys extends ReadonlyArray<AnyFamily>> =
  | readonly [StreamGroup<Keys>]
  | (Keys & readonly [AnyFamily, ...ReadonlyArray<AnyFamily>]);
