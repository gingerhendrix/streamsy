import type { StreamProtocolInterface } from "../types/protocol.ts";
export type { Clock } from "../types/storage.ts";

export interface HttpHandlerOptions {
  protocol: StreamProtocolInterface;
  pathPrefix?: string;
  maxMessageSize?: number;
}

export interface HttpHandlerInterface {
  fetch(request: Request): Promise<Response>;
}

export interface HttpRouteContext {
  request: Request;
  url: URL;
  streamId: string;
}
