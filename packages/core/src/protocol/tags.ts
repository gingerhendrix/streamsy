import { Context, type Effect } from "effect";
import type { StorageFault, StreamsFault } from "../fault.ts";
import type { StreamId } from "../schema/index.ts";
import type { AppendOptions, CreateOptions, ReadOptions, ReadNextOptions } from "./options.ts";
import type {
  AppendOutcome,
  CreateOutcome,
  HeadOutcome,
  ReadOutcome,
  ReadNextOutcome,
  RemoveOutcome,
} from "./outcomes.ts";

export interface Reader<E = StorageFault> {
  readonly head: (id: StreamId) => Effect.Effect<HeadOutcome, E>;
  readonly read: (id: StreamId, options?: ReadOptions) => Effect.Effect<ReadOutcome, E>;
  readonly readNext: (id: StreamId, options: ReadNextOptions) => Effect.Effect<ReadNextOutcome, E>;
}
export interface Writer<E = StorageFault> {
  readonly create: (id: StreamId, options?: CreateOptions) => Effect.Effect<CreateOutcome, E>;
  readonly fork: (
    id: StreamId,
    source: StreamId,
    options?: Omit<CreateOptions, "forkedFrom">,
  ) => Effect.Effect<CreateOutcome, E>;
  readonly append: (id: StreamId, options: AppendOptions) => Effect.Effect<AppendOutcome, E>;
  readonly remove: (id: StreamId) => Effect.Effect<RemoveOutcome, E>;
}
export class StreamsReader extends Context.Service<StreamsReader, Reader<StreamsFault>>()(
  "@streamsy/core/StreamsReader",
) {}
export class StreamsWriter extends Context.Service<StreamsWriter, Writer<StreamsFault>>()(
  "@streamsy/core/StreamsWriter",
) {}
