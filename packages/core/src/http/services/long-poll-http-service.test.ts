import { describe, expect, it } from "vitest";
import { LongPollHttpService } from "./long-poll-http-service.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { HttpResponseFactory } from "../responses.ts";
import type { ProtocolStream } from "../../types/protocol.ts";

describe("LongPollHttpService", () => {
  it("uses the supplied bound protocol stream", async () => {
    const stream: ProtocolStream = {
      id: "s",
      append: async () => ({ status: "appended", nextOffset: "0", currentOffset: "0" }),
      read: async () => ({ status: "ok", messages: [], nextOffset: "0", upToDate: true }),
      readLive: async () => ({
        status: "timeout",
        messages: [],
        nextOffset: "0",
        upToDate: true,
        cursor: "c",
      }),
      metadata: async () => ({ status: "ok", contentType: "text/plain", nextOffset: "0" }),
      delete: async () => ({ status: "ok" }),
    };
    const service = new LongPollHttpService({
      responses: new HttpResponseFactory(),
      bodyCodec: new MessageBodyCodec(),
    });
    const response = await service.execute(stream, "0");
    expect(response.status).toBe(204);
  });
});
