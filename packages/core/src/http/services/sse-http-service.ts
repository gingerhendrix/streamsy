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
    const contentTypeLower = metadata.contentType!.toLowerCase();
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
      metadata.nextOffset!,
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
    return new ReadableStream<Uint8Array>({
      start: async (controller) => {
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
            if (initialResult.status === "not-found") {
              controller.close();
              return;
            }
            this.writeChunks(
              controller,
              sseEvents.dataEvent(initialResult.messages, encoding),
              initialResult.messages.length > 0,
            );
            initialNextOffset = initialResult.nextOffset;
            initialUpToDate = initialResult.upToDate;
            initialClosed = initialResult.closed === true;
          }
          currentCursor = generateCursor(clock, currentCursor);
          controller.enqueue(
            sseEvents.controlEvent(
              this.buildControlData(
                initialNextOffset,
                currentCursor,
                initialUpToDate,
                initialClosed,
              ),
            ),
          );
          if (initialClosed) {
            controller.close();
            return;
          }
          currentOffset = initialNextOffset;
          while (true) {
            if (Date.now() - connectionStartTime >= CONNECTION_TIMEOUT_MS) {
              controller.close();
              return;
            }
            const result = await stream.readLive({
              offset: currentOffset,
              mode: "sse",
              cursor: currentCursor,
            });
            if (result.status === "not-found" || result.status === "not-supported") {
              controller.close();
              return;
            }
            this.writeChunks(
              controller,
              sseEvents.dataEvent(result.messages, encoding),
              result.messages.length > 0,
            );
            controller.enqueue(
              sseEvents.controlEvent(
                this.buildControlData(
                  result.nextOffset,
                  result.cursor,
                  result.upToDate,
                  result.closed === true,
                ),
              ),
            );
            currentOffset = result.nextOffset;
            currentCursor = result.cursor;
            if (result.closed) {
              controller.close();
              return;
            }
            if (result.status === "timeout") continue;
          }
        } catch (error) {
          console.error("SSE stream error:", error);
          controller.close();
        }
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

  private writeChunks(
    controller: ReadableStreamDefaultController<Uint8Array>,
    chunks: Uint8Array[],
    enabled: boolean,
  ): void {
    if (!enabled) return;
    for (const chunk of chunks) controller.enqueue(chunk);
  }
}
