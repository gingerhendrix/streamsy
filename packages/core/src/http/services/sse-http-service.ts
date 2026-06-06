import type { ProtocolStream } from "../../types/protocol.ts";
import { generateCursor } from "../../protocol/helpers/cursor-generator.ts";
import type { Clock } from "../types.ts";
import { HttpResponseFactory } from "../responses.ts";
import { SseEventEncoder, type SseEncodingOptions } from "../sse-event-encoder.ts";

const CONNECTION_TIMEOUT_MS = 60_000;

export class SseHttpService {
  constructor(
    private deps: {
      responses: HttpResponseFactory;
      sseEvents: SseEventEncoder;
      clock: Clock;
    },
  ) {}

  async execute(stream: ProtocolStream, offset: string, cursor?: string): Promise<Response> {
    const metadata = await stream.metadata();
    if (metadata.status === "not-found") return this.deps.responses.notFound();
    if (metadata.status === "gone") return this.deps.responses.gone();
    const contentTypeLower = metadata.contentType.toLowerCase();
    const encoding = {
      isText: contentTypeLower.startsWith("text/"),
      isJson: contentTypeLower.startsWith("application/json"),
      useBase64: false,
    };
    encoding.useBase64 = !encoding.isText && !encoding.isJson;
    const body = this.createStream(
      stream,
      offset,
      cursor,
      metadata.nextOffset,
      metadata.closed === true,
      encoding,
    );
    return new Response(body, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
        ...(encoding.useBase64 ? { "stream-sse-data-encoding": "base64" } : {}),
      },
    });
  }

  private createStream(
    stream: ProtocolStream,
    offset: string,
    cursor: string | undefined,
    metadataNextOffset: string,
    metadataClosed: boolean,
    encoding: SseEncodingOptions,
  ): ReadableStream<Uint8Array> {
    let currentOffset = offset;
    let currentCursor = cursor;
    const connectionStartTime = Date.now();
    const sseEvents = this.deps.sseEvents;
    const clock = this.deps.clock;
    let cancelActiveRead: (() => void) | undefined;
    return new ReadableStream<Uint8Array>({
      start: async (controller) => {
        const liveReadAbortController = new AbortController();
        let inactive = false;
        const shouldStop = () => inactive || liveReadAbortController.signal.aborted;
        const stop = () => {
          inactive = true;
          liveReadAbortController.abort();
        };
        cancelActiveRead = stop;
        const close = () => {
          if (inactive) return;
          inactive = true;
          liveReadAbortController.abort();
          try {
            controller.close();
          } catch {
            // The client may have already cancelled the response body. Treat this as a
            // normal disconnect rather than surfacing closed-controller noise.
          }
        };
        const enqueue = (chunk: Uint8Array) => {
          if (shouldStop()) return false;
          try {
            controller.enqueue(chunk);
            return true;
          } catch {
            stop();
            return false;
          }
        };
        const writeChunks = (chunks: Uint8Array[], enabled: boolean) => {
          if (!enabled) return true;
          for (const chunk of chunks) {
            if (!enqueue(chunk)) return false;
          }
          return true;
        };
        try {
          let initialNextOffset: string;
          let initialUpToDate: boolean;
          let initialClosed = false;
          if (currentOffset === "now") {
            initialNextOffset = metadataNextOffset;
            initialUpToDate = true;
            initialClosed = metadataClosed;
          } else {
            const initialResult = await stream.read({
              offset: currentOffset === "-1" ? undefined : currentOffset,
            });
            if (shouldStop()) return;
            if (initialResult.status === "not-found" || initialResult.status === "gone") {
              close();
              return;
            }
            if (
              !writeChunks(
                sseEvents.dataEvent(initialResult.messages, encoding),
                initialResult.messages.length > 0,
              )
            )
              return;
            initialNextOffset = initialResult.nextOffset;
            initialUpToDate = initialResult.upToDate;
            initialClosed = initialResult.closed === true;
          }
          currentCursor = generateCursor(clock, currentCursor);
          if (
            !enqueue(
              sseEvents.controlEvent(
                this.buildControlData(
                  initialNextOffset,
                  currentCursor,
                  initialUpToDate,
                  initialClosed,
                ),
              ),
            )
          )
            return;
          if (initialClosed) {
            close();
            return;
          }
          currentOffset = initialNextOffset;
          while (!shouldStop()) {
            if (Date.now() - connectionStartTime >= CONNECTION_TIMEOUT_MS) {
              close();
              return;
            }
            const result = await stream.readLive({
              offset: currentOffset,
              mode: "sse",
              cursor: currentCursor,
              signal: liveReadAbortController.signal,
            });
            if (shouldStop()) return;
            if (
              result.status === "not-found" ||
              result.status === "gone" ||
              result.status === "not-supported"
            ) {
              close();
              return;
            }
            if (
              !writeChunks(
                sseEvents.dataEvent(result.messages, encoding),
                result.messages.length > 0,
              )
            )
              return;
            if (
              !enqueue(
                sseEvents.controlEvent(
                  this.buildControlData(
                    result.nextOffset,
                    result.cursor,
                    result.upToDate,
                    result.closed === true,
                  ),
                ),
              )
            )
              return;
            currentOffset = result.nextOffset;
            currentCursor = result.cursor;
            if (result.closed) {
              close();
              return;
            }
            if (result.status === "timeout") continue;
          }
        } catch (error) {
          if (!shouldStop()) console.error("SSE stream error:", error);
          close();
        }
      },
      cancel: () => {
        cancelActiveRead?.();
      },
    });
  }

  private buildControlData(
    nextOffset: string,
    cursor: string | undefined,
    upToDate: boolean,
    closed: boolean,
  ): Record<string, unknown> {
    if (closed) return { streamNextOffset: nextOffset, streamClosed: true };
    return {
      streamNextOffset: nextOffset,
      streamCursor: cursor,
      ...(upToDate ? { upToDate: true } : {}),
    };
  }
}
