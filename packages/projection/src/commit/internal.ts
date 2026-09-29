/* oxlint-disable anti-slop/no-object-parameters -- Private symbol metadata accepts opaque Commit and Pending identities, without erasing their generic types. */
/* oxlint-disable typescript/no-misused-spread -- CheckpointRecord is a Schema.Struct plain data record. */
import { Effect, Option } from "effect";
import type { StreamAddress, StreamsFault } from "@streamsy/core";
import type { Reader, Writer } from "@streamsy/core/protocol";
import { CheckpointStore, type CheckpointRecord } from "../checkpoint-store.ts";
import type { Commit, Pending, Folded, Emitted, Accumulated, OutputNames } from "./model.ts";
import type { PinnedRange } from "../unit.ts";

export type Result =
  | void
  | Folded<unknown>
  | Emitted<OutputNames>
  | Accumulated<unknown, OutputNames>;
export interface ResolvedOutput {
  readonly address: StreamAddress.StreamAddress<unknown>;
  readonly service: {
    readonly owner: object;
    readonly wire: Reader<StreamsFault> & Writer<StreamsFault>;
  };
}
export interface Details {
  readonly prepare?: Effect.Effect<void, import("./model.ts").HandlerFailed>;
  readonly ranges: Record<string, PinnedRange>;
  readonly outputs: Readonly<Record<string, ResolvedOutput>>;
}
const DetailsKey = Symbol.for("@streamsy/projection/internal/Pending");
export const setDetails = <A extends object>(pending: A, value: Details): A =>
  Object.defineProperty(pending, DetailsKey, { value });
export const detailsOf = (pending: object): Details => {
  // SAFETY: only the kernel and compensation wrapper install this internal metadata.
  const value = pending as { readonly [DetailsKey]?: Details };
  return value[DetailsKey] ?? { ranges: {}, outputs: {} };
};
export { pin, isPinned, inherit } from "./range.ts";
export const load: Commit<never, never, CheckpointStore>["load"] = (key) =>
  CheckpointStore.use((store) => store.load(key));
export const remove: NonNullable<Commit<never, never, CheckpointStore>["remove"]> = (key) =>
  CheckpointStore.use((store) => store.remove(key));
export const withState = (next: CheckpointRecord, result: Result): CheckpointRecord =>
  result !== undefined && "encoded" in result ? { ...next, state: result.encoded } : next;
export const beforeRecord = (pending: Pending<unknown, unknown>): CheckpointRecord =>
  Option.getOrElse(pending.loaded.record, () => ({
    ...pending.next,
    inputs: Object.fromEntries(
      Object.entries(pending.next.inputs).map(([name, offset]) => [
        name,
        pending.unit.ranges[name]?.from ?? offset,
      ]),
    ),
  }));
export const settle = (pending: Pending<Result, unknown>, result: Result) =>
  CheckpointStore.use((store) =>
    store.compareAndSet(pending.key, pending.loaded.token, withState(pending.next, result)),
  ).pipe(Effect.asVoid);
