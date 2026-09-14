import { streamContentType } from "./config.ts";
import type { PollStats } from "./poller/contract.ts";
import type { ProjectionLimits, ProjectionStatus } from "./projection.ts";

type CurrentStats = PollStats & { readonly projection: ProjectionStatus };

type StatusResponse = CurrentStats & {
  readonly streamPath: string;
  readonly sourceStreamPath: string;
  readonly newestLimit: number;
  readonly pollIntervalMs: number;
  readonly projectionLimits: ProjectionLimits;
};

type PollResponse = CurrentStats & { readonly ok: true };
type ErrorResponse = { readonly error: string };
export type JsonResponseBody = StatusResponse | PollResponse | ErrorResponse;

export function json(value: JsonResponseBody, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  if (!headers.has("content-type")) headers.set("content-type", streamContentType);
  return new Response(JSON.stringify(value, null, 2), {
    ...init,
    headers,
  });
}
