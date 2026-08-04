import { describe, expect, it, vi } from "vitest";
import { SseHttpService } from "./sse-http-service.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { HttpResponseFactory } from "../responses.ts";
import { SseEventEncoder } from "../sse-event-encoder.ts";
import type { ProtocolStream, ReadNextOptions } from "../../types/protocol.ts";

const fixedTime = new Date("2026-06-06T00:00:00.000Z").getTime();
const clock = {
  now: () => fixedTime,
  date: (value?: number | string) => new Date(value ?? fixedTime),
};

describe("SseHttpService", () => {
  it("treats client cancellation during live reads as a normal disconnect", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    let readNextOptions: ReadNextOptions | undefined;
    let finishReadNext!: () => void;
    const readNextFinished = new Promise<void>((resolve) => {
      finishReadNext = resolve;
    });
    const stream: ProtocolStream = {
      id: "s",
      append: async () => ({ status: "appended", offset: "0" }),
      read: async () => ({ status: "ok", messages: [], nextOffset: "0", upToDate: true }),
      readNext: async (options: ReadNextOptions) => {
        readNextOptions = options;
        await new Promise<void>((resolve) => {
          if (options.signal?.aborted) {
            resolve();
            return;
          }
          options.signal?.addEventListener("abort", () => resolve(), { once: true });
        });
        finishReadNext();
        return {
          status: "timeout",
          messages: [],
          nextOffset: options.offset,
          upToDate: true,
          cursor: options.cursor ?? "c",
        };
      },
      metadata: async () => ({ status: "ok", contentType: "text/plain", nextOffset: "0" }),
      delete: async () => ({ status: "ok" }),
    };
    const service = new SseHttpService({
      responses: new HttpResponseFactory(),
      sseEvents: new SseEventEncoder(new MessageBodyCodec()),
      clock,
    });

    const response = await service.execute(stream, "now");
    expect(response.status).toBe(200);
    const reader = response.body!.getReader();
    const firstChunk = await reader.read();
    expect(firstChunk.done).toBe(false);

    await reader.cancel();
    await readNextFinished;

    expect(readNextOptions?.signal?.aborted).toBe(true);
    expect(readNextOptions).not.toHaveProperty("mode");
    expect(consoleError).not.toHaveBeenCalled();
    consoleError.mockRestore();
  });
});
