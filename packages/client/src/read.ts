import type { StreamResponse } from "@durable-streams/client";
import { ClientReadSession } from "@streamsy/core";
import type { ClientReadResult, JsonValue, ReadStreamOptions } from "@streamsy/core";
import type { OfficialProtocolClient } from "./client.ts";
import { deliverCloseOnlyEof, openReadStream } from "./compat.ts";
import { readEndFailure, readErrorResult } from "./errors.ts";

/**
 * Opens an official `StreamResponse` and bridges its `subscribe*` callbacks into
 * the shared {@link ClientReadSession}. The official client owns fetch/retry/SSE
 * decoding; this only selects the subscription by content type and maps terminal
 * outcomes to result objects.
 */
export async function officialRead<T extends JsonValue>(
  client: OfficialProtocolClient,
  url: string | URL,
  options: ReadStreamOptions,
): Promise<ClientReadResult<T>> {
  return client.run<ClientReadResult<T>>(
    options.signal,
    async (signal) => {
      const response = await openReadStream<T>({
        url,
        headers: client.options.headers,
        params: client.options.params,
        fetch: client.options.fetch,
        signal,
        backoffOptions: client.options.backoffOptions,
        offset: options.offset,
        live: options.live ?? false,
        onError: client.options.onError,
        sseResilience: client.options.sseResilience,
        warnOnHttp: client.options.warnOnHttp,
      });
      return { status: "ok", session: wrapResponse<T>(response, signal) };
    },
    readErrorResult,
  );
}

function wrapResponse<T extends JsonValue>(
  response: StreamResponse<T>,
  signal: AbortSignal,
): ClientReadSession<T> {
  const session = new ClientReadSession<T>({
    contentType: response.contentType,
    startOffset: response.startOffset,
    offset: response.startOffset,
  });
  const mediaType = response.contentType?.split(";", 1)[0]?.trim().toLowerCase();
  let deliveredBatches = 0;

  const eof = (batch: { streamClosed: boolean; upToDate: boolean }): boolean =>
    batch.streamClosed || (response.live === false && batch.upToDate);

  let unsubscribe: () => void;
  if (mediaType === "application/json") {
    unsubscribe = response.subscribeJson<T>(async (batch) => {
      await session.deliver({ kind: "json", ...batch });
      deliveredBatches++;
      if (eof(batch)) session.end({ status: "done" });
    });
  } else if (mediaType?.startsWith("text/")) {
    unsubscribe = response.subscribeText(async (batch) => {
      await session.deliver({ kind: "text", ...batch });
      deliveredBatches++;
      if (eof(batch)) session.end({ status: "done" });
    });
  } else {
    unsubscribe = response.subscribeBytes(async (batch) => {
      await session.deliver({ kind: "bytes", ...batch });
      deliveredBatches++;
      if (eof(batch)) session.end({ status: "done" });
    });
  }

  session.setCancelHook((reason) => {
    unsubscribe();
    response.cancel(reason);
  });

  // The client-level signal (aborted by `client.close()`) and any caller signal
  // terminate the session by cancelling it — the official long-poll does not
  // settle `response.closed` on a bare signal abort, so this replaces the
  // dropped session registry.
  if (signal.aborted) {
    session.cancel(signal.reason);
  } else {
    const onAbort = () => session.cancel(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void session.done.then(() => signal.removeEventListener("abort", onAbort));
  }

  // Normal termination is driven by the subscriber (`eof` above). The resolve
  // branch only synthesizes the close-only EOF that the callbacks omit; ending
  // the session unconditionally here would race with in-flight batch delivery.
  void response.closed.then(
    async () => {
      if (deliveredBatches > 0 && (await deliverCloseOnlyEof(response, session, mediaType))) {
        session.end({ status: "done" });
      }
    },
    (error: unknown) => {
      session.end(signal.aborted ? { status: "cancelled" } : readEndFailure(error, signal));
    },
  );
  return session;
}
