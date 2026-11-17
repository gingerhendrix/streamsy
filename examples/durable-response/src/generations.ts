import { Cause, Context, DateTime, Effect, FiberMap, Layer, Semaphore, Stream } from "effect";
import { LanguageModel, Response } from "effect/ai";
import {
  encodeEntry,
  finish,
  type ChatRequest,
  type StopRequest,
  type ResponseEntry,
} from "@streamsy/durable-response-contract";
import { SecureRandom } from "effect/crypto";
import { Responses } from "./streams.ts";

const make = Effect.gen(function* () {
  const responses = yield* Responses;
  const model = yield* LanguageModel.LanguageModel;
  const random = yield* SecureRandom.SecureRandom;
  const orphanStopGate = yield* Semaphore.make(1);
  const fibers = yield* FiberMap.make<string>();
  // Domain registry for currently running generations, not the durable response store.
  const running = new Map<string, { chatId: string; gate: Semaphore.Semaphore; closed: boolean }>();
  const chats = new Map<string, string>();

  const start = Effect.fn("Generations.start")(function* (input: ChatRequest) {
    const id = yield* random.uuidV4();
    const entry = { chatId: input.chatId, gate: Semaphore.makeUnsafe(1), closed: false };
    yield* responses.create({ id });
    // Keep prompt information in the stream, so another reader sees it too.
    yield* responses.append({ id }, [
      encodeEntry(
        Response.makePart("response-metadata", {
          id,
          modelId: "scripted",
          timestamp: undefined,
          metadata: { request: { prompt: input.prompt, chatId: input.chatId } },
        }),
      ),
    ]);
    running.set(id, entry);
    chats.set(input.chatId, id);

    const append = (part: ResponseEntry) =>
      entry.gate
        .withPermit(
          Effect.gen(function* () {
            if (entry.closed) return;
            const terminal = part.type === "finish" || part.type === "error";
            yield* terminal
              ? responses.append({ id }, [encodeEntry(part)], { close: true })
              : responses.append({ id }, [encodeEntry(part)]);

            if (terminal) entry.closed = true;
          }),
        )
        .pipe(Effect.uninterruptible);

    const generate = LanguageModel.streamText({ prompt: input.prompt }).pipe(
      Stream.runForEach(append),
      Effect.andThen(
        Effect.suspend(() =>
          entry.closed
            ? Effect.void
            : append(Response.makePart("error", { error: "provider ended without finish" })),
        ),
      ),
      // This is the owner/supervision boundary: provider failures become durable terminal parts.
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : append(Response.makePart("error", { error: "Generation failed" })).pipe(
              Effect.andThen(Effect.logError(cause)),
            ),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          running.delete(id);

          if (chats.get(input.chatId) === id) chats.delete(input.chatId);
        }),
      ),
      Effect.provideService(LanguageModel.LanguageModel, model),
    );

    // FiberMap is scoped to the service, never to the POST request.
    yield* FiberMap.run(fibers, id, generate);

    return { id, ...input };
  }, Effect.uninterruptible);

  const stop = Effect.fn("Generations.stop")(function* (id: string, input: StopRequest) {
    const entry = running.get(id);

    if (!entry) {
      return yield* orphanStopGate.withPermit(
        Effect.gen(function* () {
          const head = yield* responses.head({ id });

          if (head.closed) return { alreadyClosed: true };
          // A surviving open stream has no resumable provider after a host restart.
          yield* responses.append(
            { id },
            [encodeEntry(finish({ ...input, at: DateTime.formatIso(yield* DateTime.now) }))],
            { close: true },
          );

          return { alreadyClosed: false };
        }),
      );
    }

    const alreadyClosed = yield* entry.gate.withPermit(
      Effect.gen(function* () {
        if (entry.closed) return true;
        yield* responses.append(
          { id },
          [encodeEntry(finish({ ...input, at: DateTime.formatIso(yield* DateTime.now) }))],
          { close: true },
        );
        entry.closed = true;

        return false;
      }),
    );

    // Never join a provider while holding the append gate: it may be waiting on that gate.
    if (!alreadyClosed) yield* FiberMap.remove(fibers, id);

    return { alreadyClosed };
  }, Effect.uninterruptible);

  return { start, stop, active: (chatId: string) => Effect.sync(() => chats.get(chatId)) };
});

export class Generations extends Context.Service<Generations, Effect.Success<typeof make>>()(
  "DurableResponse/Generations",
) {}

export const generationsLayer = Layer.effect(Generations, make);
