/* oxlint-disable anti-slop/no-object-parameters -- Range metadata is attached to opaque Commit identities without erasing their generic contract. */
const RangeKey = Symbol.for("@streamsy/projection/internal/Range");
export const extend = <A extends object>(commit: A): A =>
  Object.defineProperty(commit, RangeKey, { value: "extend", configurable: true });
export const pin = <A extends object>(commit: A): A =>
  Object.defineProperty(commit, RangeKey, { value: "pin", configurable: true });
export const isPinned = (commit: object): boolean =>
  RangeKey in commit && commit[RangeKey] === "pin";
export const inherit = <A extends object>(source: object, target: A): A =>
  isPinned(source) ? pin(target) : extend(target);
