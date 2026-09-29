import { Data, Predicate } from "effect";
import type { Collections, CollectionsChange } from "./address.ts";

type IsUnion<T, C = T> = T extends C ? ([C] extends [T] ? false : true) : never;
type Operation<A> = Data.TaggedEnum<{
  Upsert: { readonly row: A };
  Remove: { readonly key: string | number };
}>;
/** A collection name is optional only when the route has one collection. */
export type Change<C extends Collections> = {
  [K in keyof C & string]: Operation<C[K]["schema"]["Type"]> &
    (IsUnion<keyof C> extends true ? { readonly collection: K } : { readonly collection?: K });
}[keyof C & string];
interface Operations extends Data.TaggedEnum.WithGenerics<1> {
  readonly taggedEnum: Operation<this["A"]>;
}
const operations = Data.taggedEnum<Operations>();
export function upsert<A>(row: A): Extract<Operation<A>, { readonly _tag: "Upsert" }>;
export function upsert<K extends string, A>(
  collection: K,
  row: A,
): Extract<Operation<A>, { readonly _tag: "Upsert" }> & { readonly collection: K };
export function upsert<A>(...args: [row: A] | [collection: string, row: A]) {
  return args.length === 1
    ? operations.Upsert({ row: args[0] })
    : { ...operations.Upsert({ row: args[1] }), collection: args[0] };
}
export function remove(
  key: string | number,
): Extract<Operation<never>, { readonly _tag: "Remove" }>;
export function remove<K extends string>(
  collection: K,
  key: string | number,
): Extract<Operation<never>, { readonly _tag: "Remove" }> & { readonly collection: K };
export function remove(
  ...args: [key: string | number] | [collection: string, key: string | number]
) {
  return args.length === 1
    ? operations.Remove({ key: args[0] })
    : { ...operations.Remove({ key: args[1] }), collection: args[0] };
}
/** Convert a row operation to a state event. The address codec encodes the row itself. */
export function encode<C extends Collections>(
  collections: C,
  change: Change<C>,
): CollectionsChange<C> {
  const names = Object.keys(collections);
  const name = change.collection ?? (names.length === 1 ? names[0] : undefined);
  if (name === undefined || !Object.hasOwn(collections, name))
    throw new RangeError("A row change must name a declared collection");
  const collection = collections[name];
  if (collection === undefined) throw new RangeError("Unknown collection");
  // SAFETY: Change correlates the collection name and row schema; the key comes from that collection.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The mapped union is preserved by the selected collection.
  return (
    Predicate.isTagged(change, "Upsert")
      ? {
          type: name,
          key: String(change.row[collection.key]),
          value: change.row,
          headers: { operation: "upsert" },
        }
      : { type: name, key: String(change.key), headers: { operation: "delete" } }
  ) as CollectionsChange<C>;
}
