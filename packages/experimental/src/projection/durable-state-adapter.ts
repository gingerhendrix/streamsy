import type { StreamId } from "@streamsy/core";
import { normalizeJsonCodec, type JsonSchema } from "@streamsy/json";
import type { ChangeMessage, DurableStateSchemaMap, ValuesByWireType } from "@streamsy/state";
import type {
  ProjectionAdapter,
  ProjectionCheckpoint,
  ProjectionMeta,
  ProjectionTransition,
} from "./runtime.ts";

const decoder = new TextDecoder();

export interface DurableStateProjectionRow {
  type: string;
  key: string;
  value: unknown;
}

export interface DurableStateProjectionMetaRow<State> extends ProjectionMeta {
  snapshot: State;
}

export interface DurableStateProjectionAdapterOptions<
  State,
  Event,
  Schema extends DurableStateSchemaMap,
> {
  processorId: string;
  generation: string;
  reducerVersion: string;
  sourceStreamId: StreamId;
  outputStreamId: StreamId;
  sourceSchema: JsonSchema<Event>;
  schema: Schema;
  initial(): State;
  reduce(state: State, event: Event, meta: ProjectionMeta): State;
  rows(state: State): readonly DurableStateProjectionRow[];
  /** Optional identity attached to every change in a projected source transition. */
  txid?(event: Event, meta: ProjectionMeta): string | undefined;
  meta: { type: keyof ValuesByWireType<Schema> & string; key: string };
}

function equal(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function change(
  row: DurableStateProjectionRow,
  operation: "insert" | "update",
  offset: string,
  txid?: string,
): ChangeMessage<string, unknown> {
  return { ...row, headers: { operation, offset, ...(txid ? { txid } : {}) } } as ChangeMessage<
    string,
    unknown
  >;
}

function deletion(
  row: DurableStateProjectionRow,
  offset: string,
  txid?: string,
): ChangeMessage<string, unknown> {
  return {
    type: row.type,
    key: row.key,
    value: null,
    old_value: row.value,
    headers: { operation: "delete", offset, ...(txid ? { txid } : {}) },
  } as ChangeMessage<string, unknown>;
}

/**
 * Build a ProjectionRuntime adapter from a pure reducer and Durable State rows.
 * Row diffing and the checkpoint/watermark row are framework-owned.
 */
export function durableStateProjectionAdapter<State, Event, Schema extends DurableStateSchemaMap>(
  options: DurableStateProjectionAdapterOptions<State, Event, Schema>,
): ProjectionAdapter<State, Event> {
  const sourceCodec = normalizeJsonCodec(options.sourceSchema);
  const rowCodecs = new Map(
    Object.entries(options.schema).map(([name, def]) => [
      def.type ?? name,
      normalizeJsonCodec(def.schema),
    ]),
  );

  function encodeTransition(
    transition: ProjectionTransition<State, Event>,
  ): ChangeMessage<string, unknown>[] {
    const txid = options.txid?.(transition.event, transition.meta);
    const before = new Map(
      options.rows(transition.prev).map((row) => [`${row.type}\0${row.key}`, row]),
    );
    const changes: ChangeMessage<string, unknown>[] = [];
    for (const row of options.rows(transition.next)) {
      const codec = rowCodecs.get(row.type);
      if (!codec) throw new Error(`unknown Durable State row type: ${row.type}`);
      codec.decode(row.value);
      const previous = before.get(`${row.type}\0${row.key}`);
      if (!previous) changes.push(change(row, "insert", transition.meta.sourceThroughOffset, txid));
      else if (!equal(previous.value, row.value)) {
        changes.push(change(row, "update", transition.meta.sourceThroughOffset, txid));
      }
      before.delete(`${row.type}\0${row.key}`);
    }
    for (const removed of before.values()) {
      changes.push(deletion(removed, transition.meta.sourceThroughOffset, txid));
    }
    const checkpoint: DurableStateProjectionMetaRow<State> = {
      ...transition.meta,
      snapshot: transition.next,
    };
    const metaCodec = rowCodecs.get(options.meta.type);
    if (!metaCodec) throw new Error(`unknown Durable State meta type: ${options.meta.type}`);
    metaCodec.decode(checkpoint);
    changes.push(
      change(
        { type: options.meta.type, key: options.meta.key, value: checkpoint },
        "update",
        transition.meta.sourceThroughOffset,
        txid,
      ),
    );
    return changes;
  }

  function decodeCheckpoint(messages: readonly Uint8Array[]): ProjectionCheckpoint<State> | null {
    let latest: DurableStateProjectionMetaRow<State> | null = null;
    for (const data of messages) {
      const message = JSON.parse(decoder.decode(data)) as ChangeMessage<string, unknown>;
      if (message.type === options.meta.type && message.key === options.meta.key) {
        latest = message.value as DurableStateProjectionMetaRow<State>;
      }
    }
    return latest
      ? {
          state: latest.snapshot,
          sourceThroughOffset: latest.sourceThroughOffset,
          sourceSeq: latest.sourceSeq,
        }
      : null;
  }

  return {
    processorId: options.processorId,
    generation: options.generation,
    reducerVersion: options.reducerVersion,
    sourceStreamId: options.sourceStreamId,
    outputStreamId: options.outputStreamId,
    initial: options.initial,
    decodeSourceMessage: (data) => sourceCodec.decode(JSON.parse(decoder.decode(data))),
    reduce: options.reduce,
    encodeTransition,
    decodeCheckpoint,
  };
}
