import { Record } from "effect";
import type { StreamAddress, Streamsy } from "@streamsy/core";

/** Declaration order of the map is the read and item order of every pass. */
export type InputMap = Record<string, Streamsy.Address<unknown, unknown>>;

export type ItemOf<Address> = Address extends StreamAddress.StreamAddress<infer A> ? A : never;

export interface Slice<A> {
  /** Cursor the read started after. Equal to `nextOffset` when the slice is empty. */
  readonly from: string;
  readonly items: ReadonlyArray<A>;
  /**
   * One storage offset per item, also the read cursor just after that item.
   * For a non-empty local slice, offsets.at(-1) === nextOffset. Undefined for
   * non-empty HTTP reads; empty slices always have offsets: [].
   */
  readonly offsets?: ReadonlyArray<string>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
  /** The input is gone. This final unit is followed by forgetting the binding. */
  readonly gone?: boolean;
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
    /** Storage offset of this item; reading from it resumes after the item. Absent over HTTP. */
    readonly offset?: string;
  };
}[keyof Inputs];

/** An input that was skipped this pass: nothing read, nothing known beyond its cursor. */
export const emptySlice = <A>(from: string): Slice<A> => ({
  from,
  items: [],
  offsets: [],
  nextOffset: from,
  upToDate: false,
  closed: false,
});

export type InputServices<Inputs extends InputMap> = Inputs[keyof Inputs]["stream"]["Identifier"];

/** Iteration preserves the union of the declared input stream requirements. */
export const inputEntries = <Inputs extends InputMap>(inputs: Inputs) =>
  // SAFETY: every iterated address comes from Inputs; its key is in the mapped union.
  Record.toEntries(inputs) as Array<[string, Streamsy.Address<unknown, InputServices<Inputs>>]>;
