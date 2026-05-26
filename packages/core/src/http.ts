/**
 * HTTP Handler
 *
 * Public HTTP facade that wires method-specific services to the Streamsy
 * protocol. HTTP implementation details live under ./http/.
 */

import { systemClock } from "./protocol/helpers/clock.ts";
import { HttpDispatchService } from "./http/dispatch-service.ts";
import { EtagBuilder } from "./http/etag-builder.ts";
import { MessageBodyCodec } from "./http/message-body-codec.ts";
import { ProducerHeaderParser } from "./http/producer-header-parser.ts";
import { ReadQueryParser } from "./http/read-query-parser.ts";
import { RequestBodyReader } from "./http/request-body-reader.ts";
import { HttpResponseFactory } from "./http/responses.ts";
import { SseEventEncoder } from "./http/sse-event-encoder.ts";
import { StreamPathService } from "./http/stream-path-service.ts";
import type { HttpHandlerInterface, HttpHandlerOptions } from "./http/types.ts";
import { AppendHttpService } from "./http/services/append-http-service.ts";
import { CreateHttpService } from "./http/services/create-http-service.ts";
import { DeleteHttpService } from "./http/services/delete-http-service.ts";
import { LongPollHttpService } from "./http/services/long-poll-http-service.ts";
import { MetadataHttpService } from "./http/services/metadata-http-service.ts";
import { ReadHttpService } from "./http/services/read-http-service.ts";
import { SseHttpService } from "./http/services/sse-http-service.ts";

export function createHttpHandler(options: HttpHandlerOptions): HttpHandlerInterface {
  return new HttpHandler(options);
}

export class HttpHandler implements HttpHandlerInterface {
  private dispatch: HttpDispatchService;

  constructor(options: HttpHandlerOptions) {
    const path = new StreamPathService(options.pathPrefix ?? "/");
    const responses = new HttpResponseFactory();
    const bodyReader = new RequestBodyReader(options.maxMessageSize ?? 1024 * 1024, responses);
    const bodyCodec = new MessageBodyCodec();
    const producerHeaders = new ProducerHeaderParser();
    const readQuery = new ReadQueryParser();
    const etags = new EtagBuilder();
    const sseEvents = new SseEventEncoder(bodyCodec);
    const longPoll = new LongPollHttpService({ protocol: options.protocol, responses, bodyCodec });
    const sse = new SseHttpService({
      protocol: options.protocol,
      responses,
      sseEvents,
      clock: systemClock,
    });

    this.dispatch = new HttpDispatchService({
      path,
      responses,
      create: new CreateHttpService({ protocol: options.protocol, path, responses, bodyReader }),
      append: new AppendHttpService({
        protocol: options.protocol,
        responses,
        bodyReader,
        producerHeaders,
      }),
      read: new ReadHttpService({
        protocol: options.protocol,
        responses,
        bodyCodec,
        readQuery,
        etags,
        longPoll,
        sse,
      }),
      metadata: new MetadataHttpService({ protocol: options.protocol, responses }),
      delete: new DeleteHttpService({ protocol: options.protocol, responses }),
    });
  }

  async fetch(request: Request): Promise<Response> {
    return this.dispatch.fetch(request);
  }
}
