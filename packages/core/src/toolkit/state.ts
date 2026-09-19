import type { StateChange, StateKey, StateRef } from "./ref.ts";

export interface Upsert<A> {
  readonly kind: "upsert";
  readonly value: A;
}

export interface Delete<A> {
  readonly kind: "delete";
  readonly oldValue: A;
}

export type Change<A> = Upsert<A> | Delete<A>;

/** Describe an upsert before its stream metadata and source position are applied. */
export const upsert = <A>(value: A): Upsert<A> => ({ kind: "upsert", value });

/** Describe a delete before its stream metadata and source position are applied. */
const remove = <A>(oldValue: A): Delete<A> => ({ kind: "delete", oldValue });
export { remove as delete };

/** Stamp Durable State changes with their source offset and position in this append. */
export function changes<A, RD, RE, Key extends StateKey<A>>(
  ref: StateRef<A, RD, RE, Key>,
  options: { readonly offset: string },
  entries: ReadonlyArray<Change<A>>,
): ReadonlyArray<StateChange<A>> {
  return entries.map((entry, index) => {
    const value = entry.kind === "upsert" ? entry.value : entry.oldValue;
    const rawKey = value[ref.state.key];
    if (rawKey === null || rawKey === undefined || rawKey === "") {
      throw new TypeError(
        `Invalid state key for type "${ref.state.type}": field "${String(ref.state.key)}" must not be null, undefined, or empty`,
      );
    }
    const key = String(rawKey);
    return entry.kind === "upsert"
      ? {
          type: ref.state.type,
          key,
          value,
          headers: {
            operation: "upsert",
            offset: options.offset,
            txid: `${options.offset}:${index}`,
          },
        }
      : {
          type: ref.state.type,
          key,
          old_value: value,
          headers: {
            operation: "delete",
            offset: options.offset,
            txid: `${options.offset}:${index}`,
          },
        };
  });
}
