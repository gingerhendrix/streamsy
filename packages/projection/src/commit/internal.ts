/* oxlint-disable anti-slop/no-object-parameters -- Private symbol metadata accepts opaque Commit and Pending identities, without erasing their generic types. */
/* oxlint-disable typescript/no-misused-spread -- CheckpointRecord is a Schema.Struct plain data record. */
import { Array, Effect, Option, Record } from "effect";
import type { StreamAddress, StreamsFault } from "@streamsy/core";
import type { Reader, Writer } from "@streamsy/core/protocol";
import { CheckpointStore, type CheckpointRecord } from "../checkpoint-store.ts";
import { ProjectionFault } from "../fault.ts";
import type {
  Commit,
  RetireRequest,
  Pending,
  Folded,
  Emitted,
  Accumulated,
  OutputNames,
} from "./model.ts";
import type { WakeAt } from "../wake-at.ts";
import type { PinnedRange } from "../unit.ts";

export type Result =
  | void
  | WakeAt
  | Folded<unknown>
  | Emitted<OutputNames>
  | Accumulated<unknown, OutputNames>;

export interface ResolvedOutput {
  readonly address: StreamAddress.StreamAddress<unknown>;
  readonly generation?: { readonly shared: boolean } | undefined;
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

export const retire = <R>({ key, cleanup }: RetireRequest<R>) =>
  CheckpointStore.use((store) =>
    store.retire === undefined
      ? Effect.fail(
          new ProjectionFault({
            phase: "checkpoint",
            reason: "unsupported-composition",
            message: `The checkpoint store cannot retire ${key.id}`,
          }),
        )
      : store.retire(key).pipe(Effect.tap(() => cleanup)),
  );

/** Merge the handler's state and timer into the same checkpoint write. */
export const withState = (next: CheckpointRecord, result: Result): CheckpointRecord => {
  const stored =
    result !== undefined && "encoded" in result ? { ...next, state: result.encoded } : next;

  return result !== undefined && "wakeAt" in result ? { ...stored, wakeAt: result.wakeAt } : stored;
};

export const beforeRecord = (pending: Pending<unknown, unknown>): CheckpointRecord =>
  Option.getOrElse(pending.loaded.record, () => ({
    ...pending.next,
    inputs: Record.fromEntries(
      Array.map(Record.toEntries(pending.next.inputs), ([name, offset]) => [
        name,
        pending.unit.ranges[name]?.from ?? offset,
      ]),
    ),
  }));

export const settle = (pending: Pending<Result, unknown>, result: Result) =>
  CheckpointStore.use((store) =>
    store.compareAndSet(pending.key, pending.loaded.token, withState(pending.next, result)),
  ).pipe(Effect.asVoid);
