import { compareOffsets, ZERO_OFFSET } from "@streamsy/core";
import type { Offset, StoredMessage, StreamId, StreamProtocolFactory } from "@streamsy/core";

export interface Materializer<State, Event> {
  initial: () => State;
  evolve: (state: State, event: Event, meta: RecordMeta) => State;
}

export interface RecordMeta {
  streamId: StreamId;
  offset: Offset;
  timestamp: number;
}

/** A single stream read through Streamsy's existing protocol factory seam. */
export interface StreamSource {
  protocol: StreamProtocolFactory;
  streamId: StreamId;
}

/** Open observer seam. Concrete target integrations live with their targets. */
export interface Output<State> {
  readonly kind: string;
  emit(prev: State, next: State, meta: RecordMeta): Promise<void> | void;
  appliedThrough?(): Promise<Offset | null>;
  readonly supportsFusedCheckpoint?: boolean;
}

export interface MaterializeOptions<State, Event> {
  source: StreamSource;
  decode: (message: StoredMessage, meta: RecordMeta) => Event;
  view: Materializer<State, Event>;
  from?: Offset;
  to?: Offset;
  initialState?: State;
}

export interface MaterializeResult<State> {
  state: State;
  cursor: Offset;
}

/** Fold records in the after-exclusive range `(from, to]` from one stream. */
export async function materialize<State, Event>(
  options: MaterializeOptions<State, Event>,
): Promise<MaterializeResult<State>> {
  const from = options.from ?? ZERO_OFFSET;
  if (options.to !== undefined && compareOffsets(options.to, from) < 0) {
    throw new RangeError(`Materialize 'to' offset ${options.to} precedes 'from' offset ${from}`);
  }

  const result = await options.source.protocol.get(options.source.streamId);
  if (result.status !== "ok") {
    throw new Error(
      `Cannot materialize stream ${options.source.streamId}: source status is ${result.status}`,
    );
  }

  const read = await result.stream.read({ offset: from });
  if (read.status !== "ok") {
    throw new Error(
      `Cannot materialize stream ${options.source.streamId}: read status is ${read.status}`,
    );
  }

  let state = options.initialState ?? options.view.initial();
  let cursor = from;

  for (const message of read.messages) {
    if (options.to !== undefined && compareOffsets(message.offset, options.to) > 0) break;
    const meta: RecordMeta = {
      streamId: options.source.streamId,
      offset: message.offset,
      timestamp: message.timestamp,
    };
    const event = options.decode(message, meta);
    state = options.view.evolve(state, event, meta);
    cursor = message.offset;
  }

  return { state, cursor };
}
