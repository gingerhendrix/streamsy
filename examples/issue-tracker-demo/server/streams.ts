import {
  createHttpHandler,
  createMemoryStorageAdapter,
  createStreamProtocol,
  type AppendResult,
} from "@streamsy/core";
import { createJsonProtocol, type JsonStream } from "@streamsy/json";
import type { StateEvent } from "../shared/state-schema.ts";
import { contentType } from "./config.ts";

const streamPrefix = "/streams";

// Events are validated by the server before they are appended; reads trust
// the durable log, so the codec is a plain identity passthrough.
const eventCodec = {
  encode: (value: StateEvent): unknown => value,
  decode: (value: unknown): StateEvent => value as StateEvent,
};

/**
 * Multi-stream access to the demo's Streamsy protocol: one durable stream per
 * workspace (`workspace/<id>`), resolved per request — no per-stream state is
 * cached here, keeping the server stateless. The protocol is the only source
 * of truth for which workspaces exist.
 */
export class DemoStreams {
  private readonly protocol = createStreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
  });
  private readonly json = createJsonProtocol<StateEvent>(this.protocol, eventCodec);
  private readonly handler = createHttpHandler({
    protocol: this.protocol,
    pathPrefix: streamPrefix,
  });

  /** Create the stream if it does not exist yet; no-op when it already does. */
  async ensureStream(streamId: string): Promise<void> {
    const result = await this.protocol.create(streamId, { contentType });
    if (result.status !== "created" && result.status !== "exists") {
      throw new Error(`Unable to create Streamsy stream ${streamId}: ${result.status}`);
    }
  }

  /** Resolve a typed JSON stream, or undefined when the stream does not exist. */
  async getJsonStream(streamId: string): Promise<JsonStream<StateEvent> | undefined> {
    const result = await this.json.get(streamId);
    if (result.status === "not-found" || result.status === "gone") return undefined;
    if (result.status !== "ok") {
      throw new Error(`Unable to resolve Streamsy stream ${streamId}: ${result.status}`);
    }
    return result.stream;
  }

  /**
   * Append one state event, optionally guarded by an `expectedOffset` CAS
   * precondition. Returns the raw {@link AppendResult} so callers can drive
   * retry loops on `expected-offset` conflicts.
   */
  async appendEvent(
    streamId: string,
    event: StateEvent,
    expectedOffset?: string,
  ): Promise<AppendResult> {
    const stream = await this.getJsonStream(streamId);
    if (!stream) {
      throw new Error(`Streamsy stream not found: ${streamId}`);
    }
    return stream.append(event, expectedOffset === undefined ? {} : { expectedOffset });
  }

  /**
   * Read a stream from the beginning to its current head. Returns the decoded
   * events plus the head offset — the CAS token for appends conditioned on
   * exactly this state — or undefined when the stream does not exist.
   */
  async readAll(
    streamId: string,
  ): Promise<{ events: StateEvent[]; headOffset: string } | undefined> {
    const stream = await this.getJsonStream(streamId);
    if (!stream) return undefined;

    const events: StateEvent[] = [];
    let offset: string | undefined;
    for (;;) {
      const result = await stream.read(offset === undefined ? {} : { offset });
      if (result.status !== "ok") {
        throw new Error(`Unable to read Streamsy stream ${streamId}: ${result.status}`);
      }
      for (const message of result.messages) {
        events.push(message.value);
      }
      offset = result.nextOffset;
      if (result.upToDate) {
        return { events, headOffset: result.nextOffset };
      }
    }
  }

  async proxy(request: Request): Promise<Response> {
    console.log(`Proxying stream request: ${request.method} ${request.url}`);
    return this.handler.fetch(request);
  }
}
