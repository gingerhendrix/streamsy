import { Data, Predicate, type Types } from "effect";
import type { NonEmptyReadonlyArray } from "effect/Array";
import type {
  Collections,
  CollectionsChange,
  StateHeaders,
  StateKey,
  StateAddress,
} from "./address.ts";

export interface ChangeHeaders {
  readonly txid?: string;
  readonly timestamp?: string;
  readonly from?: string;
}

type Operation<A, Type extends string> = Data.TaggedEnum<{
  Upsert: { readonly type: Type; readonly value: A; readonly headers?: ChangeHeaders };
  Delete: {
    readonly type: Type;
    readonly key: string | number;
    readonly oldValue?: A;
    readonly headers?: ChangeHeaders;
  };
}>;

interface Operations extends Data.TaggedEnum.WithGenerics<2> {
  readonly taggedEnum: Operation<this["A"], this["B"] & string>;
}

const operations = Data.taggedEnum<Operations>();

export type Upsert<A, Type extends string = string> = Data.TaggedEnum.Value<
  Operation<A, Type>,
  "Upsert"
>;

export type Delete<A, Type extends string = string> = Data.TaggedEnum.Value<
  Operation<A, Type>,
  "Delete"
>;

/** A pending change for one of the collections in `C`. */
export type Change<C extends Collections> = {
  [K in keyof C & string]: Upsert<C[K]["schema"]["Type"], K> | Delete<C[K]["schema"]["Type"], K>;
}[keyof C & string];

/** Describe an upsert into a collection. The key is read from the value. */
export const upsert = <const Type extends string, A>(
  type: Type,
  value: A,
  options: { readonly headers?: ChangeHeaders } = {},
): Upsert<A, Type> => operations.Upsert({ type, value, ...options });

/** Describe a delete from a collection by key. An `oldValue` is written as `old_value`. */
export const remove = <const Type extends string, A = never>(
  type: Type,
  key: string | number,
  options: { readonly oldValue?: A; readonly headers?: ChangeHeaders } = {},
): Delete<NoInfer<A>, Type> => operations.Delete({ type, key, ...options });

/** Reject keys that Durable State clients cannot address. */
const checkKey = <Raw>(type: string, source: string, raw: Raw): string => {
  if (raw === null || raw === undefined || raw === "") {
    throw new TypeError(
      `Invalid state key for type "${type}": ${source} must not be null, undefined, or empty`,
    );
  }

  return String(raw);
};

/** Read the key field of a value. `StateKey` limits the field to strings and numbers. */
const readKey = <A, Key extends StateKey<A>>(type: string, field: Key, value: A): string =>
  checkKey(type, `field "${String(field)}"`, value[field]);

/**
 * Stamp Durable State changes with their key and headers. With an `offset`, each
 * change gets that offset and a default txid of `<offset>:<index>`.
 */
export function changes<C extends Collections, RD, RE>(
  address: StateAddress<C, RD, RE>,
  options: { readonly offset?: string },
  entries: NonEmptyReadonlyArray<Change<C>>,
): NonEmptyReadonlyArray<CollectionsChange<C>>;
export function changes<C extends Collections, RD, RE>(
  address: StateAddress<C, RD, RE>,
  options: { readonly offset?: string },
  entries: ReadonlyArray<Change<C>>,
): ReadonlyArray<CollectionsChange<C>>;
export function changes<C extends Collections, RD, RE>(
  address: StateAddress<C, RD, RE>,
  options: { readonly offset?: string },
  entries: ReadonlyArray<Change<C>>,
): ReadonlyArray<CollectionsChange<C>> {
  const { offset } = options;

  return entries.map((entry, index): CollectionsChange<C> => {
    const collection = Object.hasOwn(address.collections, entry.type)
      ? address.collections[entry.type]
      : undefined;

    if (collection === undefined) throw new TypeError(`Unknown state collection "${entry.type}"`);
    // Only the three overridable fields are read from the entry, so a wider
    // value cannot replace `offset` and an explicit `undefined` cannot drop `txid`.
    const override = entry.headers ?? {};
    const txid = override.txid ?? (offset === undefined ? undefined : `${offset}:${index}`);
    const headers: Types.Mutable<Omit<StateHeaders, "operation">> = {};

    if (offset !== undefined) headers.offset = offset;

    if (txid !== undefined) headers.txid = txid;

    if (override.timestamp !== undefined) headers.timestamp = override.timestamp;

    if (override.from !== undefined) headers.from = override.from;

    if (Predicate.isTagged(entry, "Upsert")) {
      const key = readKey(entry.type, collection.key, entry.value);

      return {
        type: entry.type,
        key,
        value: entry.value,
        headers: { operation: "upsert", ...headers },
      };
    }

    const key = checkKey(entry.type, "key", entry.key);

    return entry.oldValue === undefined
      ? { type: entry.type, key, headers: { operation: "delete", ...headers } }
      : {
          type: entry.type,
          key,
          old_value: entry.oldValue,
          headers: { operation: "delete", ...headers },
        };
  });
}
