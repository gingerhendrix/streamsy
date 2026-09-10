import { Context, type Effect } from "effect";
import type { StorageFault, StreamsFault } from "../fault.ts";
import type { StreamId } from "../schema/index.ts";
import type { AppendOptions, CreateOptions, ReadOptions, ReadNextOptions } from "./options.ts";
import type {
  HeadError,
  ReadError,
  ReadNextError,
  CreateError,
  AppendError,
  RemoveError,
} from "./errors.ts";
import type {
  AppendResult,
  CreateResult,
  HeadResult,
  ReadResult,
  ReadNextResult,
} from "./results.ts";

export interface Reader<E = StorageFault> {
  readonly head: (id: StreamId) => Effect.Effect<HeadResult, HeadError | E>;
  readonly read: (id: StreamId, options?: ReadOptions) => Effect.Effect<ReadResult, ReadError | E>;
  readonly readNext: (
    id: StreamId,
    options: ReadNextOptions,
  ) => Effect.Effect<ReadNextResult, ReadNextError | E>;
}
export interface Writer<E = StorageFault> {
  readonly create: (
    id: StreamId,
    options?: CreateOptions,
  ) => Effect.Effect<CreateResult, CreateError | E>;
  readonly fork: (
    id: StreamId,
    source: StreamId,
    options?: Omit<CreateOptions, "forkedFrom">,
  ) => Effect.Effect<CreateResult, CreateError | E>;
  readonly append: (
    id: StreamId,
    options: AppendOptions,
  ) => Effect.Effect<AppendResult, AppendError | E>;
  readonly remove: (id: StreamId) => Effect.Effect<void, RemoveError | E>;
}
export class StreamsReader extends Context.Service<StreamsReader, Reader<StreamsFault>>()(
  "@streamsy/core/StreamsReader",
) {}
export class StreamsWriter extends Context.Service<StreamsWriter, Writer<StreamsFault>>()(
  "@streamsy/core/StreamsWriter",
) {}
