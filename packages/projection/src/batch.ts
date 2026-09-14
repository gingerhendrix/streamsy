import type { StreamRef } from "@streamsy/core";

/** Declaration order of the map is the read and item order of every pass. */
export type InputMap = Record<string, StreamRef.StreamRef<unknown>>;
export type ItemOf<Ref> = Ref extends StreamRef.StreamRef<infer A> ? A : never;

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
