import type { ProtocolStream } from "../../types/protocol.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { maybeNotSupportedResponse } from "../not-supported.ts";
import { HttpResponseFactory } from "../responses.ts";

export class LongPollHttpService {
  constructor(
    private deps: {
      responses: HttpResponseFactory;
      bodyCodec: MessageBodyCodec;
    },
  ) {}

  async execute(stream: ProtocolStream, offset: string, cursor?: string): Promise<Response> {
    const result = await stream.readLive({
      offset,
      mode: "long-poll",
      cursor,
    });
    if (result.status === "not-supported")
      return maybeNotSupportedResponse(result, this.deps.responses)!;
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    if (result.messages.length === 0) return this.toNoContentResponse(result);
    const metadata = await stream.metadata();
    if (metadata.status === "not-found") return this.deps.responses.notFound();
    if (metadata.status === "gone") return this.deps.responses.gone();
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
