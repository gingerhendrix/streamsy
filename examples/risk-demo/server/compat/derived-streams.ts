/* oxlint-disable effecttsgo/async-function -- This module preserves a public Promise compatibility facade over protocol/runtime-owned application work. */
import { ZERO_OFFSET, type StreamProtocolFactory } from "@streamsy/core";
import {
  createJsonProtocol,
  normalizeJsonCodec,
  type JsonReadAllResult,
  type JsonSchema,
  type JsonStoredMessage,
} from "@streamsy/core/json";

export interface DerivedSourceMessage<T> extends JsonStoredMessage<T> {}

export interface CatchUpDerivedOptions<Source, Key, Output> {
  protocol: StreamProtocolFactory;
  sourceStreamId: string;
  sourceSchema: JsonSchema<Source>;
  outputSchema: JsonSchema<Output>;
  derive(messages: readonly DerivedSourceMessage<Source>[]): Map<Key, Output[]>;
  streamIdFor(key: Key): string;
  producerIdFor(key: Key): string;
  /** Maximum reload attempts after duplicate/CAS conflicts. Defaults to 8. */
  maxAttempts?: number;
}

const DEFAULT_MAX_ATTEMPTS = 8;
type JsonSourceValue = {} | null | undefined;

function assertDurablePrefix<Output>(
  streamId: string,
  desired: readonly Output[],
  history: JsonReadAllResult<Output>,
  encode: (value: Output) => JsonSourceValue,
): void {
  if (history.messages.length > desired.length) {
    throw new Error(
      `derived stream ${streamId} has ${history.messages.length} durable messages, ` +
        `but deterministic output has only ${desired.length}`,
    );
  }
  for (let seq = 0; seq < history.messages.length; seq += 1) {
    const durable = JSON.stringify(encode(history.messages[seq]!.value));
    const expected = JSON.stringify(encode(desired[seq]!));
    if (durable !== expected) {
      throw new Error(
        `derived stream ${streamId} diverged at producer sequence ${seq}: ` +
          "a different durable payload won",
      );
    }
  }
}

export async function catchUpDerived<Source, Key, Output>(
  options: CatchUpDerivedOptions<Source, Key, Output>,
): Promise<void> {
  const sourceProtocol = createJsonProtocol(options.protocol, options.sourceSchema);
  const source = await sourceProtocol.get(options.sourceStreamId);
  if (source.status === "not-found") return;
  if (source.status !== "ok") throw new Error(`cannot read derived source: ${source.status}`);
  const sourceHistory = await source.stream.readAll();
  const outputProtocol = createJsonProtocol(options.protocol, options.outputSchema);
  const outputCodec = normalizeJsonCodec(options.outputSchema);

  for (const [key, desired] of options.derive(sourceHistory.messages)) {
    const stream = await outputProtocol.getOrCreate(options.streamIdFor(key));
    let history = await stream.readAll();
    assertDurablePrefix(stream.id, desired, history, (value) => outputCodec.encode(value));
    let attempts = 0;
    for (let seq = history.messages.length; seq < desired.length; seq += 1) {
      const result = await stream.append(desired[seq]!, {
        producer: { producerId: options.producerIdFor(key), producerEpoch: 1, producerSeq: seq },
        expectedOffset: history.head,
      });
      if (result.status === "appended") {
        history = { ...history, head: result.offset };
        continue;
      }
      if (
        result.status === "duplicate" ||
        (result.status === "conflict" && result.conflictReason === "expected-offset")
      ) {
        attempts += 1;
        if (attempts >= (options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS)) {
          throw new Error(
            `derived append for ${stream.id} could not converge after bounded retries`,
          );
        }
        history = await stream.readAll();
        assertDurablePrefix(stream.id, desired, history, (value) => outputCodec.encode(value));
        seq = history.messages.length - 1;
        continue;
      }
      throw new Error(`derived append failed for ${stream.id}: ${result.status}`);
    }
  }
}

export async function readDerived<T>(
  protocol: StreamProtocolFactory,
  streamId: string,
  schema: JsonSchema<T>,
  options: { cursor?: string; waitMs?: number; signal?: AbortSignal } = {},
): Promise<{ values: T[]; cursor: string; upToDate: boolean }> {
  const stream = await createJsonProtocol(protocol, schema).getOrCreate(streamId);
  const offset = options.cursor ?? ZERO_OFFSET;
  const read = await stream.read({ offset });
  if (read.status === "ok" && read.messages.length > 0) {
    return {
      values: read.messages.map((message) => message.value),
      cursor: read.nextOffset,
      upToDate: read.upToDate,
    };
  }
  if (options.waitMs && options.waitMs > 0) {
    const timeout = AbortSignal.timeout(options.waitMs);
    const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
    const live = await stream.readNext({
      offset,
      signal,
    });
    if (live.status === "invalid-json") throw new Error(`cannot decode derived stream ${streamId}`);
    if (live.status !== "not-supported" && live.messages.length > 0) {
      return {
        values: live.messages.map((message) => message.value),
        cursor: live.nextOffset,
        upToDate: live.upToDate,
      };
    }
    return {
      values: [],
      cursor: live.status === "not-supported" ? offset : live.nextOffset,
      upToDate: true,
    };
  }
  return { values: [], cursor: read.status === "ok" ? read.nextOffset : offset, upToDate: true };
}
