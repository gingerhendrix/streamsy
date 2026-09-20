import { Effect, Stream } from "effect";
import type { HttpServerRequest } from "effect/unstable/http";

/**
 * Consumes an unread request body before an early rejection.
 *
 * An HTTP/1.1 host can only reuse a connection when the request body was read
 * to its end. A body that is dropped or cancelled counts as unread, so the host
 * closes the connection after the response, and a client that reuses the pooled
 * connection at that moment sees a socket close. Under workerd this showed up
 * as the conformance case that sends four rejected appends back to back.
 *
 * The body is read through the Effect stream and discarded chunk by chunk, so
 * nothing is buffered. An absent body fails the stream and a locked body
 * defects on `getReader`; both are ignored because the rejection response is
 * the outcome that matters.
 */
export const discardRequestBody = (
  request: HttpServerRequest.HttpServerRequest,
): Effect.Effect<void> => Stream.runDrain(request.stream).pipe(Effect.ignoreCause);
