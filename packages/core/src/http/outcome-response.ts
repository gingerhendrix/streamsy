import type {
  AppendOutcome,
  CreateOutcome,
  HeadOutcome,
  ReadOutcome,
  ReadNextOutcome,
  RemoveOutcome,
} from "../protocol/outcomes.ts";
import { format, resultHeader } from "../protocol/remote-format.ts";

type Outcome =
  | AppendOutcome
  | CreateOutcome
  | HeadOutcome
  | ReadOutcome
  | ReadNextOutcome
  | RemoveOutcome;

/** Preserve metadata only when an Effect transport explicitly requests it. */
export function outcomeResponse(result: Outcome, response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set("vary", "accept");
  headers.set("cache-control", "no-store");
  if ("messages" in result) {
    headers.set("content-type", format);
    return new Response(
      JSON.stringify({
        ...result,
        messages: result.messages.map((message) => ({
          offset: message.offset,
          timestamp: message.timestamp,
          data: Array.from(message.data),
        })),
      }),
      { status: response.status === 204 ? 200 : response.status, headers },
    );
  }
  headers.set(resultHeader, encodeURIComponent(JSON.stringify(result)));
  return new Response(response.body, { status: response.status, headers });
}
