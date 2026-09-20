import type { Collections, CollectionsChange, StateHeaders, StateKey, StateRef } from "./ref.ts";

export interface Upsert<A, Type extends string = string> {
  readonly kind: "upsert";
  readonly type: Type;
  readonly value: A;
  readonly headers?: ChangeHeaders;
}

export interface Delete<A, Type extends string = string> {
  readonly kind: "delete";
  readonly type: Type;
  readonly oldValue: A;
  readonly headers?: ChangeHeaders;
}

export interface ChangeHeaders {
  readonly txid?: string;
  readonly timestamp?: string;
  readonly from?: string;
}

/** A pending change for one of the collections in `C`. */
export type Change<C extends Collections> = {
  [K in keyof C & string]: Upsert<C[K]["schema"]["Type"], K> | Delete<C[K]["schema"]["Type"], K>;
}[keyof C & string];

/** Describe an upsert into a collection before its key and source position are applied. */
export const upsert = <const Type extends string, A>(
  type: Type,
  value: A,
  options: { readonly headers?: ChangeHeaders } = {},
): Upsert<A, Type> => ({ kind: "upsert", type, value, ...options });

/** Describe a delete from a collection before its key and source position are applied. */
const remove = <const Type extends string, A>(
  type: Type,
  oldValue: A,
  options: { readonly headers?: ChangeHeaders } = {},
): Delete<A, Type> => ({ kind: "delete", type, oldValue, ...options });
export { remove as delete };

/** Read the key field of a value. `StateKey` limits the field to strings and numbers. */
const readKey = <A, Key extends StateKey<A>>(type: string, field: Key, value: A): string => {
  const raw = value[field];
  if (raw === null || raw === undefined || raw === "") {
    throw new TypeError(
      `Invalid state key for type "${type}": field "${String(field)}" must not be null, undefined, or empty`,
    );
  }
  return String(raw);
};

/** Stamp Durable State changes with their key, source offset, and position in this append. */
export function changes<C extends Collections, RD, RE>(
  ref: StateRef<C, RD, RE>,
  options: { readonly offset: string },
  entries: ReadonlyArray<Change<C>>,
): ReadonlyArray<CollectionsChange<C>> {
  return entries.map((entry, index): CollectionsChange<C> => {
    const collection = ref.collections[entry.type];
    if (collection === undefined) throw new TypeError(`Unknown state collection "${entry.type}"`);
    const value = entry.kind === "upsert" ? entry.value : entry.oldValue;
    const key = readKey(entry.type, collection.key, value);
    // Only the three overridable fields are read from the entry, so a wider
    // value cannot replace `offset` and an explicit `undefined` cannot drop `txid`.
    const override = entry.headers ?? {};
    const stamped = { offset: options.offset, txid: override.txid ?? `${options.offset}:${index}` };
    const withTimestamp =
      override.timestamp === undefined ? stamped : { ...stamped, timestamp: override.timestamp };
    const headers = (
      override.from === undefined ? withTimestamp : { ...withTimestamp, from: override.from }
    ) satisfies Omit<StateHeaders, "operation">;
    return entry.kind === "upsert"
      ? { type: entry.type, key, value, headers: { operation: "upsert", ...headers } }
      : { type: entry.type, key, old_value: value, headers: { operation: "delete", ...headers } };
  });
}
