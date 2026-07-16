import type { ProtocolStream } from "../../types/protocol.ts";
import { encodeBatch } from "../batch-encoding.ts";
import { ClientReadSession } from "../read-session.ts";
import type { ClientReadResult, JsonValue, ReadStreamOptions } from "../types.ts";
import { combineSignals, type DirectProtocolClient } from "./client.ts";
import { absentResult, failureFromThrown, readFailure } from "./results.ts";

const START = "-1";

/**
 * Resolves a read into a session and drives the catch-up (and optional live)
 * pump. The pump delivers content-aware batches and never throws into the
 * consumer: it ends the session with a {@link ReadEndResult}.
 */
export async function directRead<T extends JsonValue>(
  client: DirectProtocolClient,
  id: string,
  options: ReadStreamOptions,
): Promise<ClientReadResult<T>> {
  return client.run(options.signal, async (operationSignal) => {
    const found = await client.streamsy.get(id);
    if (found.status !== "ok") return absentResult(found);
    const metadata = await found.stream.metadata();
    if (metadata.status !== "ok") return absentResult(metadata);

    const controller = new AbortController();
    const signal = combineSignals(operationSignal, controller.signal);
    const session = new ClientReadSession<T>({
      contentType: metadata.contentType,
      startOffset: options.offset ?? START,
      onCancel: (reason) => controller.abort(reason),
    });
    void pump(found.stream, metadata.contentType, options, signal, session);
    return { status: "ok", session };
  });
}

async function pump<T extends JsonValue>(
  stream: ProtocolStream,
  contentType: string,
  options: ReadStreamOptions,
  signal: AbortSignal,
  session: ClientReadSession<T>,
): Promise<void> {
  let offset = options.offset ?? START;
  let cursor: string | undefined;
  const finish = () => session.end(signal.aborted ? { status: "cancelled" } : { status: "done" });
  try {
    while (!signal.aborted) {
      const result = await stream.read({ offset });
      if (result.status !== "ok") return session.end(readFailure(result));
      const batch = encodeBatch<T>(contentType, result.messages, {
        offset: result.nextOffset,
        upToDate: result.upToDate,
        streamClosed: result.closed === true,
      });
      await session.deliver(batch);
      offset = batch.offset;
      if (batch.streamClosed || batch.upToDate) break;
    }

    if (options.live !== "long-poll" && options.live !== "sse") return finish();

    while (!signal.aborted && !session.streamClosed) {
      const result = await stream.readLive({ offset, mode: options.live, cursor, signal });
      if (result.status !== "ok" && result.status !== "timeout") {
        return session.end(readFailure(result));
      }
      const batch = encodeBatch<T>(contentType, result.messages, {
        offset: result.nextOffset,
        cursor: result.cursor,
        upToDate: result.upToDate,
        streamClosed: result.closed === true,
      });
      await session.deliver(batch);
      offset = batch.offset;
      cursor = batch.cursor;
      if (batch.streamClosed) break;
    }
    finish();
  } catch (error) {
    if (signal.aborted) session.end({ status: "cancelled" });
    else session.end(failureFromThrown(error));
  }
}
