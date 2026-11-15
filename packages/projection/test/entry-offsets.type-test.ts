import type { Streamsy } from "@streamsy/core";
import type { Entry, Slice } from "@streamsy/projection";

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

export type OffsetChecks = [
  Assert<Same<Entry<{ input: Streamsy.Address<number, never> }>["offset"], string | undefined>>,
  Assert<Same<Slice<number>["offsets"], ReadonlyArray<string> | undefined>>,
];
