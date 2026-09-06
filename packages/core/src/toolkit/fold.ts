import { Effect, Stream } from "effect";
import type { StreamRef } from "./ref.ts";
import * as Streams from "./streams.ts";
/** Completes at close; interruption releases an open stream's parked read. */
export const run = Effect.fn("Fold.run")(function* <A, S, RD, RE>(
  ref: StreamRef<A, RD, RE>,
  options: { readonly initial: S; readonly step: (state: S, item: A) => S; readonly from?: string },
) {
  return yield* Streams.follow(ref, { offset: options.from }).pipe(
    Streams.items,
    Stream.runFold(() => options.initial, options.step),
  );
});
