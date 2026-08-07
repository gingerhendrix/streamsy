/**
 * Command edge.
 *
 * Every command carries a `commandId`. That id becomes a producer lane on the
 * target stream, so a retried command is reconciled by the protocol's producer
 * tuple and returns the exact offset the original append received. Payload
 * equality is never claimed.
 */
import type { ClientProducerOptions } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { sourceAck, type SourceAck } from "@streamsy/experimental/causal";
import { AppendStreams, type AppendOutcome } from "@streamsy/experimental/effect";
import { Effect, Schema } from "effect";
import { assertIdentifier, IssueEvent, ProjectMembershipFact } from "../shared/domain.ts";

export type CommandAppend =
  | { readonly status: "accepted"; readonly ack: SourceAck }
  /** The producer sequence was already accepted; the original offset is exact. */
  | { readonly status: "reconciled"; readonly ack: SourceAck }
  | { readonly status: "rejected"; readonly outcome: AppendOutcome };

const encoder = new TextEncoder();

/** Derive a bounded, deterministic producer lane for one command id. */
export async function commandProducer(commandId: string): Promise<ClientProducerOptions> {
  if (typeof commandId !== "string" || commandId.length === 0 || commandId.length > 128) {
    throw new TypeError("commandId must be a non-empty string of at most 128 characters");
  }
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(commandId));
  const hex = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
  return { producerId: `issue-tracker-cmd-${hex.slice(0, 40)}`, producerEpoch: 0, producerSeq: 0 };
}

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
  return classify(source, result);
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
  return classify(membership, result);
});

function classify(binding: StreamBinding, result: AppendOutcome): CommandAppend {
  if (result.status === "appended") {
    return { status: "accepted", ack: sourceAck(binding.identity, result.offset) };
  }
  if (result.status === "duplicate") {
    return { status: "reconciled", ack: sourceAck(binding.identity, result.offset) };
  }
  return { status: "rejected", outcome: result };
}

/**
 * Build the deterministic workflow producer id for the membership half of issue
 * creation, so a repeated repair of a partly complete creation is safe.
 */
export function membershipCommandId(commandId: string, issueId: string): string {
  return `${assertIdentifier(issueId, "issueId")}:membership:${commandId}`;
}
