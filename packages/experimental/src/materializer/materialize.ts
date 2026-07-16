import type {
  JsonValue,
  StreamBatch,
  StreamId,
  StreamOffset,
  StreamProtocolClient,
} from "@streamsy/core";

export interface Materializer<State, Event> {
  initial: () => State;
  evolve: (state: State, event: Event, meta: BatchMeta) => State;
}

/** Metadata shared by every event decoded from one client delivery batch. */
export interface BatchMeta {
  streamId: StreamId;
  /** After-exclusive resume token for the complete batch. */
  offset: StreamOffset;
}

/** A single stream read through the transport-neutral protocol client seam. */
export interface StreamSource {
  client: StreamProtocolClient;
  streamId: StreamId;
}

/** Open observer seam. Concrete target integrations live with their targets. */
export interface Output<State> {
  readonly kind: string;
  emit(prev: State, next: State, meta: BatchMeta): Promise<void> | void;
  appliedThrough?(): Promise<StreamOffset | null>;
  readonly supportsFusedCheckpoint?: boolean;
}

export interface MaterializeOptions<State, Event, Json extends JsonValue = JsonValue> {
  source: StreamSource;
  /** Decode one content-aware client batch into zero or more domain events. */
  decode: (batch: StreamBatch<Json>, meta: BatchMeta) => Iterable<Event>;
  view: Materializer<State, Event>;
  /** After-exclusive client resume token. Omission reads from the stream start. */
  from?: StreamOffset;
  initialState?: State;
}

export interface MaterializeResult<State> {
  state: State;
  /** After-exclusive token at the last completely consumed delivery batch. */
  cursor: StreamOffset;
}

/**
 * Fold all currently available batches from one stream.
 *
 * The client owns transport, content decoding, and resume-token semantics. This
 * fold rejects on source/read/session, decode, or evolve failure and commits
 * nothing, so callers can safely resume from the last persisted batch cursor.
 */
export async function materialize<State, Event, Json extends JsonValue = JsonValue>(
  options: MaterializeOptions<State, Event, Json>,
): Promise<MaterializeResult<State>> {
  const handle = options.source.client.stream(options.source.streamId);
  const read = await handle.read<Json>({ offset: options.from, live: false });
  if (read.status !== "ok") {
    throw new Error(
      `Cannot materialize stream ${options.source.streamId}: read status is ${describe(read)}`,
    );
  }

  let state = options.initialState ?? options.view.initial();
  let cursor = read.session.offset;

  for await (const batch of read.session) {
    const meta: BatchMeta = {
      streamId: options.source.streamId,
      offset: batch.offset,
    };
    for (const event of options.decode(batch, meta)) {
      state = options.view.evolve(state, event, meta);
    }
    cursor = batch.offset;
  }

  const end = await read.session.done;
  if (end.status !== "done") {
    throw new Error(
      `Cannot materialize stream ${options.source.streamId}: session status is ${describe(end)}`,
    );
  }

  return { state, cursor };
}

function describe(result: { status: string; code?: string }): string {
  return result.code === undefined ? result.status : `${result.status} (${result.code})`;
}
