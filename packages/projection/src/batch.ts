import { Record } from "effect";
import type { StreamAddress, Streamsy } from "@streamsy/core";

/** Declaration order of the map is the read and item order of every pass. */
export type InputMap = Record<string, Streamsy.Address<unknown, unknown>>;

export type ItemOf<Address> = Address extends StreamAddress.StreamAddress<infer A> ? A : never;

export interface Slice<A> {
  /** Cursor the read started after. Equal to `nextOffset` when the slice is empty. */
  readonly from: string;
  readonly items: ReadonlyArray<A>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

export type Slices<Inputs extends InputMap> = {
  readonly [K in keyof Inputs]: Slice<ItemOf<Inputs[K]>>;
};

/** One item tagged with its input; `index` is its position in the whole unit. */
export type Entry<Inputs extends InputMap> = {
  readonly [K in keyof Inputs]: {
    readonly input: K;
    readonly item: ItemOf<Inputs[K]>;
    readonly index: number;
  };
}[keyof Inputs];

/** An input that was skipped this pass: nothing read, nothing known beyond its cursor. */
export const emptySlice = <A>(from: string): Slice<A> => ({
  from,
  items: [],
  nextOffset: from,
  upToDate: false,
  closed: false,
});

export type InputServices<Inputs extends InputMap> = Inputs[keyof Inputs]["stream"]["Identifier"];

/** Iteration preserves the union of the declared input stream requirements. */
export const inputEntries = <Inputs extends InputMap>(inputs: Inputs) =>
  // SAFETY: every iterated address comes from Inputs; its key is in the mapped union.
  Record.toEntries(inputs) as Array<[string, Streamsy.Address<unknown, InputServices<Inputs>>]>;
