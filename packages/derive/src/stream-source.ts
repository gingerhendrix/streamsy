import { encodeKey } from "./identity.ts";
/* oxlint-disable eslint/no-underscore-dangle -- Effect results and stream refs use public tagged variants. */
import { Effect, Option, Schema } from "effect";
import { Offset, StreamsReader, ZERO_OFFSET, type StreamRef } from "@streamsy/core";
import { DeriveFault } from "./fault.ts";
import type { Boundary, Source } from "./source.ts";

/** Direct services only. Retain source history and never reuse a deleted stream id. */
export const make = Effect.fn("Derive.StreamSource.make")(function* <A>(
  ref: StreamRef.StreamRef<A>,
) {
  const reader = yield* StreamsReader;
  const storageFault = () =>
    new DeriveFault({ reason: "storage-failure", message: `Cannot read ${ref.id}` });
  return {
    identity: encodeKey([ref.id, ref.contentType]),
    initialPosition: ZERO_OFFSET,
    pull: Effect.fn("Derive.StreamSource.pull")(function* (after, limits) {
      if (Option.isNone(Schema.decodeOption(Offset)(after)))
        return yield* new DeriveFault({
          reason: "invalid-state",
          message: "Stored source offset is invalid",
        });
      const result = yield* reader
        .read(ref.id, { offset: after, limit: limits.items })
        .pipe(Effect.mapError(storageFault));
      if (
        result.status !== "ok" ||
        result.nextOffset < after ||
        (result.messages.length === 0 && result.nextOffset !== after)
      )
        return { status: "history-unavailable" } as const;
      const bytes = result.messages.reduce((sum, message) => sum + message.data.byteLength, 0);
      // Never split an accepted source boundary or claim bytes that were not accepted.
      if (limits.bytes !== undefined && bytes > limits.bytes)
        return { status: "limit-reached" } as const;
      const items = yield* Effect.forEach(result.messages, (message) =>
        Schema.decodeEffect(ref.codec)(
          ref._tag === "Json" ? new TextDecoder().decode(message.data) : message.data,
        ).pipe(
          Effect.mapError(
            () =>
              new DeriveFault({
                reason: "invalid-source",
                message: `Cannot decode ${ref.id} at ${message.offset}`,
              }),
          ),
        ),
      );
      return {
        status: "boundary",
        items,
        endPosition: result.nextOffset,
        bytes,
        upToDate: result.upToDate,
        closed: result.closed === true,
      } satisfies Boundary<A>;
    }),
    wait: Effect.fn("Derive.StreamSource.wait")((after) =>
      reader.readNext(ref.id, { offset: after }).pipe(Effect.asVoid, Effect.mapError(storageFault)),
    ),
  } satisfies Source<A>;
});
