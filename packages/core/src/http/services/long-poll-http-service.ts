import type { StreamProtocolInterface } from "../../types/protocol.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { HttpResponseFactory } from "../responses.ts";

export class LongPollHttpService {
  constructor(
    private deps: {
      protocol: StreamProtocolInterface;
      responses: HttpResponseFactory;
      bodyCodec: MessageBodyCodec;
    },
  ) {}

  async execute(streamId: string, offset: string, cursor?: string): Promise<Response> {
    const result = await this.deps.protocol.readLive(streamId, {
      offset,
      mode: "long-poll",
      cursor,
    });
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    if (result.messages.length === 0) return this.toNoContentResponse(result);
    const metadata = await this.deps.protocol.metadata(streamId);
    return new Response(
      this.deps.bodyCodec.encodeHttpBody(result.messages, metadata.contentType!),
      {
        headers: {
          "content-type": metadata.contentType!,
          "stream-next-offset": result.nextOffset,
          "stream-up-to-date": "true",
          ...(result.closed ? {} : { "stream-cursor": result.cursor }),
          ...(result.closed ? { "stream-closed": "true" } : {}),
        },
      },
    );
  }

  private toNoContentResponse(result: {
    nextOffset: string;
    cursor: string;
    closed?: boolean;
  }): Response {
    return this.deps.responses.empty(204, {
      "stream-next-offset": result.nextOffset,
      "stream-up-to-date": "true",
      ...(result.closed ? {} : { "stream-cursor": result.cursor }),
      ...(result.closed ? { "stream-closed": "true" } : {}),
    });
  }
}
