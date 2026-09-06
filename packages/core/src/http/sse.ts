import { Clock, Effect, Random, Stream } from "effect";
import { HttpServerResponse } from "effect/unstable/http";
import type { Reader } from "../protocol/tags.ts";
import type { StreamId } from "../schema/index.ts";
import { generateCursor } from "../policy/cursor-generator.ts";
import { MessageBodyCodec } from "./message-body-codec.ts";
import { SseEventEncoder, type SseControlData } from "./sse-event-encoder.ts";

const events = new SseEventEncoder(new MessageBodyCodec());

/** Pull-based batches are unbounded in message count, like readNext and toolkit follow. */
export function sse(
  reader: Reader,
  id: StreamId,
  contentType: string,
  offset: string,
  cursor?: string,
) {
  const encoding = {
    isJson: contentType.toLowerCase().startsWith("application/json"),
    isText: contentType.toLowerCase().startsWith("text/"),
    useBase64: false,
  };
  encoding.useBase64 = !encoding.isJson && !encoding.isText;
  const frames = Stream.suspend(() => {
    let initial = true;
    let currentOffset = offset;
    let currentCursor = cursor;
    return Stream.fromEffectRepeat(
      Effect.gen(function* () {
        const result = initial
          ? yield* reader.read(id, { offset: currentOffset })
          : yield* reader.readNext(id, { offset: currentOffset, cursor: currentCursor });
        if (
          result.status === "not-found" ||
          result.status === "gone" ||
          result.status === "not-supported"
        )
          return { chunks: [], done: true };
        if (initial) {
          const now = yield* Clock.currentTimeMillis;
          const random = yield* Random.next;
          currentCursor = generateCursor({ now: () => now }, currentCursor, () => random);
        } else if ("cursor" in result) currentCursor = result.cursor;
        initial = false;
        currentOffset = result.nextOffset;
        const chunks = result.messages.length ? events.dataEvent(result.messages, encoding) : [];
        const control: SseControlData = result.closed
          ? { streamNextOffset: currentOffset, streamClosed: true }
          : { streamNextOffset: currentOffset, streamCursor: currentCursor };
        if (!result.closed && result.upToDate) control.upToDate = true;
        chunks.push(events.controlEvent(control));
        return { chunks, done: result.closed === true };
      }),
    ).pipe(
      Stream.takeUntil((batch) => batch.done),
      Stream.flatMap((batch) => Stream.fromIterable(batch.chunks)),
      Stream.interruptWhen(Effect.sleep(60_000)),
    );
  });
  const headers = new Headers({
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
  if (encoding.useBase64) headers.set("stream-sse-data-encoding", "base64");
  return HttpServerResponse.stream(frames, { headers: Object.fromEntries(headers) });
}
