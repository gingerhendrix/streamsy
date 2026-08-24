/**
 * Command edge.
 *
 * Every command carries a `commandId`. That id becomes a producer lane on the
 * issue-events stream, so a retried command is reconciled by the protocol's
 * producer tuple and returns the exact offset the original append received.
 * Payload equality is never claimed, and no second event is appended.
 *
 * Deriving a lane hashes the command id through WebCrypto, which is async. It
 * is wrapped once, here, so no application workflow contains a Promise call.
 */
import type { ClientProducerOptions } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { AppendStreams, type AppendOutcome } from "@streamsy/experimental/effect";
import { Cache, Context, Effect, Layer } from "effect";
import { encodeIssueEventJson, type IssueEvent } from "../domain/issue.ts";
import { AppendRejected, InvalidRequest } from "./errors.ts";

export type CommandAppend =
  | { readonly status: "accepted"; readonly offset: string }
  /** The producer sequence was already accepted; `offset` is the original one. */
  | { readonly status: "reconciled"; readonly offset: string };

const encoder = new TextEncoder();

/** How many command lanes one runtime keeps derived. */
const PRODUCER_CAPACITY = 4_096;

export interface CommandProducerResolver {
  readonly forCommand: (commandId: string) => Effect.Effect<ClientProducerOptions, InvalidRequest>;
}

export class CommandProducers extends Context.Service<CommandProducers, CommandProducerResolver>()(
  "issue-tracker/CommandProducers",
) {}

const digest = (commandId: string): Effect.Effect<ClientProducerOptions> =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", encoder.encode(commandId))).pipe(
    Effect.map((bytes) => {
      const hex = Array.from(new Uint8Array(bytes), (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join("");
      return {
        producerId: `issue-tracker-cmd-${hex.slice(0, 40)}`,
        producerEpoch: 0,
        producerSeq: 0,
      };
    }),
  );

export const layer: Layer.Layer<CommandProducers> = Layer.effect(
  CommandProducers,
  Effect.gen(function* () {
    const cache = yield* Cache.make<string, ClientProducerOptions>({
      capacity: PRODUCER_CAPACITY,
      lookup: digest,
    });
    return CommandProducers.of({
      forCommand: Effect.fn("CommandProducers.forCommand")(function* (commandId: string) {
        if (commandId.length === 0 || commandId.length > 128) {
          return yield* InvalidRequest.of("commandId", "must be 1 to 128 characters long");
        }
        return yield* Cache.get(cache, commandId);
      }),
    });
  }),
);

/** Append one canonical issue event on the command's own producer lane. */
export const appendIssueEvent = Effect.fn("Commands.appendIssueEvent")(function* (
  source: StreamBinding,
  event: IssueEvent,
  producer: ClientProducerOptions,
) {
  const appends = yield* AppendStreams;
  const payload = yield* Effect.sync(() => encodeIssueEventJson(event));
  const result = yield* appends.append(source, payload, {
    contentType: "application/json",
    producer,
  });
  return yield* classify(source, result);
});

/**
 * Acceptance and producer reconciliation are the only success cases. Every
 * other protocol outcome is a typed `AppendRejected`, so no caller can mistake
 * a refused append for a durable one.
 */
function classify(
  binding: StreamBinding,
  result: AppendOutcome,
): Effect.Effect<CommandAppend, AppendRejected> {
  if (result.status === "appended") {
    return Effect.succeed({ status: "accepted", offset: result.offset });
  }
  if (result.status === "duplicate") {
    return Effect.succeed({ status: "reconciled", offset: result.offset });
  }
  return Effect.fail(new AppendRejected({ stream: binding.streamId, status: result.status }));
}
