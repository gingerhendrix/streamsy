import { SecureRandom } from "effect/crypto";
import { Effect } from "effect";
import { AtomRegistry } from "effect/reactivity";
import { startChat, stopResponse } from "@streamsy/durable-response-contract/client";
import { atomRuntime } from "./runtime.ts";
import { busyAtom, noticeAtom, promptAtom, readerIdAtom, responseIdAtom } from "./state.ts";

export const startAtom = atomRuntime.fn<void>()(
  Effect.fnUntraced(function* () {
    const registry = yield* AtomRegistry.AtomRegistry;

    if (registry.get(busyAtom)) return;
    registry.set(busyAtom, true);
    const chatId = yield* (yield* SecureRandom.SecureRandom).uuidV4();
    yield* startChat({ prompt: registry.get(promptAtom), chatId }).pipe(
      Effect.tap(({ id }) => Effect.sync(() => window.location.assign(`/r/${id}`))),
      Effect.catch(() => Effect.sync(() => registry.set(noticeAtom, "Unable to start response"))),
      Effect.ensuring(Effect.sync(() => registry.set(busyAtom, false))),
    );
  }),
);

export const stopAtom = atomRuntime.fn<void>()(
  Effect.fnUntraced(function* () {
    const registry = yield* AtomRegistry.AtomRegistry;
    yield* stopResponse(registry.get(responseIdAtom), {
      by: registry.get(readerIdAtom),
      reason: "Stopped from browser",
    }).pipe(
      Effect.tap(({ alreadyClosed }) =>
        Effect.sync(() =>
          registry.set(noticeAtom, alreadyClosed ? "Already finished" : "Stop requested"),
        ),
      ),
      Effect.catch(() => Effect.sync(() => registry.set(noticeAtom, "Unable to stop response"))),
    );
  }),
);
