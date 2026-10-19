import type { StreamsFault } from "../fault.ts";
import { Clock, Effect, Random, Record, Stream } from "effect";
import { HttpServerResponse } from "effect/http";
import type { Reader } from "../protocol/tags.ts";
import type { StreamId } from "../schema/index.ts";
import { generateCursor } from "../policy/cursor-generator.ts";
import * as SseEvents from "./sse-event-encoder.ts";
import type { SseControlData } from "./sse-event-encoder.ts";
import { concatBytes } from "./message-body-codec.ts";
import type { ShutdownSignalService } from "./shutdown-signal.ts";

const events = SseEvents;

/** Each connection's deadline is `deadlineMs` scaled by a factor from 0.9 to 1.1. */
export const DEADLINE_JITTER = 0.1;

/**
 * Spread `deadlineMs` by up to `DEADLINE_JITTER` either way, from one draw `r`
 * in [0, 1). Connections opened together then do not all close together.
 */
export function jitterDeadline(deadlineMs: number, r: number): number {
  return Math.round(deadlineMs * (1 - DEADLINE_JITTER + 2 * DEADLINE_JITTER * r));
}

/** Pull-based batches are unbounded in message count, like readNext and toolkit follow. */
export function sse(
  reader: Reader<StreamsFault>,
  id: StreamId,
  contentType: string,
  offset: string,
  cursor?: string,
  deadlineMs = 60_000,
  signal?: ShutdownSignalService,
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
          ? yield* reader
              .read(id, { offset: currentOffset })
              .pipe(Effect.map((batch) => ({ ...batch, cursor: undefined })))
          : yield* reader.readNext(id, { offset: currentOffset, cursor: currentCursor });

        if (initial) {
          const now = yield* Clock.currentTimeMillis;
          const random = yield* Random.next;
          currentCursor = generateCursor({ now: () => now }, currentCursor, () => random);
        } else if (result.cursor !== undefined) currentCursor = result.cursor;
        initial = false;
        currentOffset = result.nextOffset;
        const chunks = result.messages.length ? events.dataEvent(result.messages, encoding) : [];

        const control: SseControlData = result.closed
          ? { streamNextOffset: currentOffset, streamClosed: true }
          : { streamNextOffset: currentOffset, streamCursor: currentCursor };

        if (!result.closed && result.upToDate) control.upToDate = true;
        chunks.push(events.controlEvent(control));
        // One chunk per batch: the deadline merge below can then end the body only
        // between batches, never between a data event and the control event that
        // moves the client's offset past it.
        const frame = concatBytes(chunks.map((data) => ({ data })));

        return { frames: [frame], done: result.closed };
      }).pipe(
        Effect.catchTags({
          StreamNotFound: () => Effect.succeed({ frames: [], done: true }),
          StreamGone: () => Effect.succeed({ frames: [], done: true }),
          NotSupported: () => Effect.succeed({ frames: [], done: true }),
        }),
      ),
    ).pipe(
      Stream.takeUntil((batch) => batch.done),
      Stream.flatMap((batch) => Stream.fromIterable(batch.frames)),
      Stream.merge(
        Stream.fromEffect(
          Random.next.pipe(Effect.flatMap((r) => Effect.sleep(jitterDeadline(deadlineMs, r)))),
        ).pipe(Stream.drain),
        { haltStrategy: "either" },
      ),
      // A host drain ends the body the same way as the deadline: after a whole batch.
      Stream.merge(Stream.fromEffect(signal?.draining ?? Effect.never).pipe(Stream.drain), {
        haltStrategy: "either",
      }),
    );
  });

  const headers = new Headers({
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });

  if (encoding.useBase64) headers.set("stream-sse-data-encoding", "base64");
  const body = signal === undefined ? frames : signal.trackStream(frames);

  return HttpServerResponse.stream(body, { headers: Record.fromEntries(headers) });
}
