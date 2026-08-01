import type { ProtocolStream, StreamProtocolFactory } from "../types/protocol.ts";
export type { Clock } from "../types/storage.ts";

export interface HttpHandlerOptions {
  protocol: StreamProtocolFactory;
  pathPrefix?: string;
  maxMessageSize?: number;
  /** Catch-up and stable long-poll cache visibility. Defaults to `private`. */
  cacheVisibility?: HttpCacheVisibility;
}

export interface ReadOnlyHttpHandlerOptions {
  protocol: StreamProtocolFactory;
  pathPrefix?: string;
  /** Catch-up and stable long-poll cache visibility. Defaults to `private`. */
  cacheVisibility?: HttpCacheVisibility;
}

export type HttpCacheVisibility = "private" | "public";

export interface HttpHandlerInterface {
  fetch(request: Request): Promise<Response>;
}

export interface HttpRouteContext {
  request: Request;
  url: URL;
  streamId: string;
}

export interface BoundHttpRouteContext extends HttpRouteContext {
  stream: ProtocolStream;
}
