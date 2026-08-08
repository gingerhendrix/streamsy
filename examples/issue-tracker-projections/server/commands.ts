/**
 * Command edge.
 *
 * Every command carries a `commandId`. That id becomes a producer lane on the
 * target stream, so a retried command is reconciled by the protocol's producer
 * tuple and returns the exact offset the original append received. Payload
 * equality is never claimed.
 *
 * Deriving the lane hashes the command id, which is the platform's async
 * WebCrypto API. It is wrapped once, inside `CommandProducers`, so no
 * application workflow contains a Promise call.
 */
import type { ClientProducerOptions } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { sourceAck, type SourceAck } from "@streamsy/experimental/causal";
import { AppendStreams, type AppendOutcome } from "@streamsy/experimental/effect";
import { Cache, Context, Effect, Layer, Schema } from "effect";
import { IssueEvent, ProjectMembershipFact } from "../shared/domain.ts";
import { AppendRejected, InvalidRequest } from "./errors.ts";

export type CommandAppend =
  | { readonly status: "accepted"; readonly ack: SourceAck }
  /** The producer sequence was already accepted; the original offset is exact. */
  | { readonly status: "reconciled"; readonly ack: SourceAck };

const encoder = new TextEncoder();

/** How many command lanes one runtime keeps derived. */
const PRODUCER_CAPACITY = 4_096;

export interface CommandProducersShape {
  /** Derive the bounded, deterministic producer lane for one command id. */
  readonly forCommand: (commandId: string) => Effect.Effect<ClientProducerOptions, InvalidRequest>;
}

export class CommandProducers extends Context.Service<CommandProducers, CommandProducersShape>()(
  "issue-tracker-projections/CommandProducers",
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
          return yield* Effect.fail(
            InvalidRequest.of("commandId", "must be 1 to 128 characters long"),
          );
        }
        return yield* Cache.get(cache, commandId);
      }),
    });
  }),
);

const encodeIssueEvent = Schema.encodeUnknownSync(IssueEvent);
const encodeMembershipFact = Schema.encodeUnknownSync(ProjectMembershipFact);

export const appendIssueEvent = Effect.fn("Commands.appendIssueEvent")(function* (
  source: StreamBinding,
  event: IssueEvent,
  producer: ClientProducerOptions,
) {
  const appends = yield* AppendStreams;
  const payload = yield* Effect.sync(() => JSON.stringify(encodeIssueEvent(event)));
  const result = yield* appends.append(source, payload, {
    contentType: "application/json",
    producer,
  });
  return yield* classify(source, result);
});

export const appendMembershipFact = Effect.fn("Commands.appendMembershipFact")(function* (
  membership: StreamBinding,
  fact: ProjectMembershipFact,
  producer: ClientProducerOptions,
) {
  const appends = yield* AppendStreams;
  const payload = yield* Effect.sync(() => JSON.stringify(encodeMembershipFact(fact)));
  const result = yield* appends.append(membership, payload, {
    contentType: "application/json",
    producer,
  });
  return yield* classify(membership, result);
});

/**
 * Acceptance and producer reconciliation are the only success cases. Every other
 * protocol outcome is a typed `AppendRejected`, so no caller can mistake a
 * refused append for a durable one.
 */
function classify(
  binding: StreamBinding,
  result: AppendOutcome,
): Effect.Effect<CommandAppend, AppendRejected> {
  if (result.status === "appended") {
    return Effect.succeed({ status: "accepted", ack: sourceAck(binding.identity, result.offset) });
  }
  if (result.status === "duplicate") {
    return Effect.succeed({
      status: "reconciled",
      ack: sourceAck(binding.identity, result.offset),
    });
  }
  return Effect.fail(new AppendRejected({ stream: binding.streamId, status: result.status }));
}

/**
 * Build the deterministic workflow producer id for the membership half of issue
 * creation, so a repeated repair of a partly complete creation is safe.
 */
export function membershipCommandId(commandId: string, issueId: string): string {
  return `${issueId}:membership:${commandId}`;
}
