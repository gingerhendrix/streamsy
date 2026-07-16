/**
 * Read-only HTTP Handler
 *
 * Public HTTP facade for stream consumers. It exposes the existing read and
 * metadata routes while rejecting HTTP write methods at the handler boundary.
 */

import { systemClock } from "./protocol/helpers/clock.ts";
import { EtagBuilder } from "./http/etag-builder.ts";
import { MessageBodyCodec } from "./http/message-body-codec.ts";
import { ReadOnlyHttpDispatchService } from "./http/read-only-dispatch-service.ts";
import { ReadQueryParser } from "./http/read-query-parser.ts";
import { HttpResponseFactory } from "./http/responses.ts";
import { SseEventEncoder } from "./http/sse-event-encoder.ts";
import { StreamPathService } from "./http/stream-path-service.ts";
import type { HttpHandlerInterface, ReadOnlyHttpHandlerOptions } from "./http/types.ts";
import { LongPollHttpService } from "./http/services/long-poll-http-service.ts";
import { MetadataHttpService } from "./http/services/metadata-http-service.ts";
import { ReadHttpService } from "./http/services/read-http-service.ts";
import { SseHttpService } from "./http/services/sse-http-service.ts";

export function createReadOnlyHttpHandler(
  options: ReadOnlyHttpHandlerOptions,
): HttpHandlerInterface {
  return new ReadOnlyHttpHandler(options);
}

export class ReadOnlyHttpHandler implements HttpHandlerInterface {
  private dispatch: ReadOnlyHttpDispatchService;

  constructor(options: ReadOnlyHttpHandlerOptions) {
    const path = new StreamPathService(options.pathPrefix ?? "/");
    const responses = new HttpResponseFactory();
    const bodyCodec = new MessageBodyCodec();
    const readQuery = new ReadQueryParser((offset) => options.protocol.isValidOffset(offset));
    const etags = new EtagBuilder();
    const sseEvents = new SseEventEncoder(bodyCodec);
    const longPoll = new LongPollHttpService({ responses, bodyCodec });
    const sse = new SseHttpService({
      responses,
      sseEvents,
      clock: systemClock,
    });

    this.dispatch = new ReadOnlyHttpDispatchService({
      protocol: options.protocol,
      path,
      responses,
      read: new ReadHttpService({
        responses,
        bodyCodec,
        readQuery,
        etags,
        longPoll,
        sse,
      }),
      metadata: new MetadataHttpService({ responses }),
    });
  }

  async fetch(request: Request): Promise<Response> {
    return this.dispatch.fetch(request);
  }
}
