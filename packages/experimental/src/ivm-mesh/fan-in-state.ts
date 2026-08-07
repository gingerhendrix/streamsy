import type { JsonValue, StreamBatch } from "@streamsy/core";
import { Context, Effect, Layer } from "effect";
import type { StreamBinding } from "../binding.ts";
import {
  decodeStreamIdentity,
  encodeStreamIdentity,
  sourceAck,
  streamIdentityEquals,
  type StreamIdentity,
} from "../causal.ts";
import {
  IncompatibleLineage,
  MalformedLineage,
  MalformedSourceBoundary,
  ProjectionPoison,
  StateRestorePoison,
  type MeshOperationalError,
} from "../effect/errors.ts";
import {
  AppendStreams,
  ReadStreams,
  type AppendOutcome,
  type ReadStreamsShape,
} from "../effect/streams.ts";
import { assertStateFactShape, type AppendDerivedStateResult } from "./derived-append.ts";
import {
  compareMembers,
  createFanInCheckpoint,
  createMemberRow,
  decodeFanInCheckpoint,
  decodeFanInMember,
  ensureFanInCheckpointCompatible,
  FAN_IN_CHECKPOINT_TYPE,
  FAN_IN_MEMBER_TYPE,
  isReservedType,
  MEMBER_KEY_PREFIX,
  removeMemberRow,
  type FanInMember,
} from "./fan-in-meta.ts";
import type { ProducerLane } from "./lane.ts";
import type { CatchUpLimits, ProjectionBoundary } from "./projection.ts";
import { assertFactTypeAllowed } from "./state-meta.ts";

export type { FanInMember } from "./fan-in-meta.ts";

/** One decoded membership transition. `from` declares the member start position. */
export type MembershipChange =
  | { readonly type: "join"; readonly member: StreamIdentity; readonly from?: string }
  | { readonly type: "leave"; readonly member: StreamIdentity };

export interface FanInCheckpoint {
  readonly targetOffset: string;
  readonly membershipThrough: string | null;
  readonly nextProducerSeq: number;
  readonly producerId: string;
  readonly producerEpoch: number;
}

export interface RecoveredFanInState {
  readonly status: "ready";
  readonly checkpoint: FanInCheckpoint;
  readonly members: readonly FanInMember[];
  readonly facts: readonly JsonValue[];
}

export type FanInRecoveryResult = RecoveredFanInState | { readonly status: "not-found" | "gone" };

export interface FanInRecoveryShape {
  readonly recoverFanIn: (
    target: StreamBinding,
    lane: ProducerLane,
  ) => Effect.Effect<FanInRecoveryResult, MeshOperationalError>;
}

export class FanInRecovery extends Context.Service<FanInRecovery, FanInRecoveryShape>()(
  "@streamsy/experimental/FanInRecovery",
) {}

export const FanInRecoveryLive = Layer.effect(
  FanInRecovery,
  Effect.gen(function* () {
    const reads = yield* ReadStreams;
    return FanInRecovery.of({ recoverFanIn: makeFanInScan(reads) });
  }),
);

export const FanInRecoveryTest = (recoverFanIn: FanInRecoveryShape["recoverFanIn"]) =>
  Layer.succeed(FanInRecovery, FanInRecovery.of({ recoverFanIn }));

export interface FanInStepResult<State> {
  readonly state: State;
  readonly facts: readonly JsonValue[];
}

export interface CatchUpFanInStateOptions<State, MemberInput> {
  /** The one membership source that governs the active member set. */
  readonly membership: StreamBinding;
  readonly target: StreamBinding;
  /** `lane.source` must be the membership identity; `lane.target` the State target. */
  readonly lane: ProducerLane;
  readonly limits: CatchUpLimits;
  readonly initial: State;
  readonly restore: (initial: State, events: readonly JsonValue[]) => State;
  readonly decodeMembership: (
    batch: StreamBatch,
    boundary: ProjectionBoundary,
  ) => Iterable<MembershipChange>;
  /** Resolve a durable member identity to a live binding, or `undefined`. */
  readonly resolveMember: (identity: StreamIdentity) => StreamBinding | undefined;
  readonly decodeMember: (
    batch: StreamBatch,
    member: FanInMember,
    boundary: ProjectionBoundary,
  ) => Iterable<MemberInput>;
  readonly onRecord: (
    state: State,
    member: FanInMember,
    input: readonly MemberInput[],
    boundary: ProjectionBoundary,
  ) => FanInStepResult<State>;
  /** Removal policy applied when a member leaves. */
  readonly onRemove: (state: State, member: FanInMember) => FanInStepResult<State>;
}

export interface CatchUpFanInProgress<State> {
  readonly checkpoint: FanInCheckpoint;
  readonly members: readonly FanInMember[];
  readonly state: State;
  readonly pages: number;
  readonly batches: number;
  readonly items: number;
  readonly bytes: number;
}

export type CatchUpFanInResult<State> =
  | ({ readonly status: "caught-up" } & CatchUpFanInProgress<State>)
  | ({
      readonly status: "limit-reached";
      readonly limit: keyof CatchUpLimits;
    } & CatchUpFanInProgress<State>)
  | ({
      readonly status: "boundary-too-large";
      readonly limit: "maxItems" | "maxBytes";
      readonly stream: "membership" | "member";
      readonly actual: number;
      readonly maximum: number;
    } & CatchUpFanInProgress<State>)
  | {
      readonly status: "missing" | "gone";
      readonly stream: "membership" | "target" | "member";
      readonly member?: string;
    }
  | ({ readonly status: "unknown-member"; readonly member: string } & CatchUpFanInProgress<State>)
  | ({
      readonly status: "output-conflict";
      readonly reason: string;
      readonly offset?: string;
    } & CatchUpFanInProgress<State>)
  | (Extract<
      AppendDerivedStateResult,
      { readonly status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }
    > &
      CatchUpFanInProgress<State>);

/**
 * Bounded dynamic fan-in State kernel: one membership source, many member
 * streams, one State target.
 *
 * Selection is level-triggered and deterministic. The membership boundary is
 * always incorporated first; otherwise the ready member with the lowest
 * canonical encoded identity supplies exactly one complete boundary. The rule
 * therefore reproduces the same order after any restart.
 *
 * Every transaction commits application facts, changed member rows, and the
 * fan-in checkpoint in one append, so a membership boundary and its effects can
 * never be partly durable. Member scanning is O(active members) per committed
 * boundary; snapshots and an indexed ready-set are deliberately deferred.
 */
export const catchUpDynamicFanInState = Effect.fn("catchUpDynamicFanInState")(
  <State, MemberInput>(options: CatchUpFanInStateOptions<State, MemberInput>) =>
    Effect.gen(function* () {
      validateFanInOptions(options);
      const recovery = yield* FanInRecovery;
      const recovered = yield* recovery.recoverFanIn(options.target, options.lane);
      if (recovered.status !== "ready") {
        return {
          status: recovered.status === "not-found" ? ("missing" as const) : ("gone" as const),
          stream: "target" as const,
        };
      }
      const state = yield* Effect.try({
        try: () => options.restore(options.initial, recovered.facts),
        catch: (cause) =>
          new StateRestorePoison({ targetOffset: recovered.checkpoint.targetOffset, cause }),
      });
      return yield* runFanInPasses(options, {
        checkpoint: recovered.checkpoint,
        members: recovered.members,
        state,
        pages: 0,
        batches: 0,
        items: 0,
        bytes: 0,
      });
    }),
);

const runFanInPasses = <State, MemberInput>(
  options: CatchUpFanInStateOptions<State, MemberInput>,
  start: CatchUpFanInProgress<State>,
): Effect.Effect<
  CatchUpFanInResult<State>,
  MeshOperationalError,
  ReadStreams | AppendStreams | FanInRecovery
> =>
  Effect.gen(function* () {
    let progress = start;
    while (true) {
      if (progress.pages >= options.limits.maxPages)
        return { status: "limit-reached" as const, limit: "maxPages" as const, ...progress };
      if (progress.batches >= options.limits.maxBatches)
        return { status: "limit-reached" as const, limit: "maxBatches" as const, ...progress };

      const membershipStep = yield* advanceMembership(options, progress);
      if (membershipStep.kind === "result") return membershipStep.result;
      if (membershipStep.kind === "advanced") {
        progress = membershipStep.progress;
        continue;
      }

      const memberStep = yield* advanceMembers(options, progress);
      if (memberStep.kind === "result") return memberStep.result;
      if (memberStep.kind === "advanced") {
        progress = memberStep.progress;
        continue;
      }
      return { status: "caught-up" as const, ...progress };
    }
  });

type Step<State> =
  | { readonly kind: "idle" }
  | { readonly kind: "advanced"; readonly progress: CatchUpFanInProgress<State> }
  | { readonly kind: "result"; readonly result: CatchUpFanInResult<State> };

const advanceMembership = <State, MemberInput>(
  options: CatchUpFanInStateOptions<State, MemberInput>,
  progress: CatchUpFanInProgress<State>,
): Effect.Effect<Step<State>, MeshOperationalError, ReadStreams | AppendStreams | FanInRecovery> =>
  Effect.gen(function* () {
    const opened = yield* readNextBoundary(
      options.membership,
      progress.checkpoint.membershipThrough,
    );
    if (opened.status === "missing" || opened.status === "gone") {
      return {
        kind: "result" as const,
        result: { status: opened.status, stream: "membership" as const },
      };
    }
    if (opened.status === "idle") return { kind: "idle" as const };

    const batch = opened.batch;
    const ack = yield* Effect.try({
      try: () => sourceAck(options.membership.identity, batch.offset),
      catch: (cause) => new MalformedSourceBoundary({ offset: batch.offset, cause }),
    });
    const bytes = encodedBatchBytes(batch);
    const sizing = boundarySizing(options.limits, progress, bytes, "membership");
    if (sizing) return { kind: "result" as const, result: { ...sizing, ...progress } };
    const boundary: ProjectionBoundary = { source: ack, page: progress.pages + 1, bytes };

    const changes = yield* Effect.try({
      try: () => Array.from(options.decodeMembership(batch, boundary)),
      catch: (cause) =>
        new ProjectionPoison({ phase: "membership", sourcePosition: ack.position, cause }),
    });
    const itemLimit = itemSizing(options.limits, progress, changes.length, "membership");
    if (itemLimit) return { kind: "result" as const, result: { ...itemLimit, ...progress } };

    const members = new Map(
      progress.members.map((member) => [encodeStreamIdentity(member.identity), member] as const),
    );
    const facts: JsonValue[] = [];
    const meta: JsonValue[] = [];
    let state = progress.state;
    for (const change of changes) {
      const key = encodeStreamIdentity(change.member);
      if (change.type === "join") {
        if (members.has(key)) continue;
        const member: FanInMember = {
          identity: change.member,
          from: change.from ?? null,
          through: null,
        };
        members.set(key, member);
        meta.push(createMemberRow(member) as unknown as JsonValue);
        continue;
      }
      const existing = members.get(key);
      if (!existing) continue;
      const removed = yield* Effect.try({
        try: () => options.onRemove(state, existing),
        catch: (cause) =>
          new ProjectionPoison({ phase: "remove", sourcePosition: ack.position, cause }),
      });
      state = removed.state;
      facts.push(...removed.facts);
      members.delete(key);
      meta.push(removeMemberRow(existing.identity));
    }

    const appended = yield* appendFanInBatch(options, progress.checkpoint, {
      facts,
      meta,
      membershipThrough: ack.position,
    });
    if (appended.kind === "failed") {
      return { kind: "result" as const, result: mergeFailure(appended.outcome, progress) };
    }
    return {
      kind: "advanced" as const,
      progress: {
        checkpoint: appended.checkpoint,
        members: sortMembers(Array.from(members.values())),
        state,
        pages: progress.pages + 1,
        batches: progress.batches + 1,
        items: progress.items + changes.length,
        bytes: progress.bytes + bytes,
      },
    };
  });

const advanceMembers = <State, MemberInput>(
  options: CatchUpFanInStateOptions<State, MemberInput>,
  progress: CatchUpFanInProgress<State>,
): Effect.Effect<Step<State>, MeshOperationalError, ReadStreams | AppendStreams | FanInRecovery> =>
  Effect.gen(function* () {
    for (const member of progress.members) {
      const binding = options.resolveMember(member.identity);
      if (binding === undefined) {
        return {
          kind: "result" as const,
          result: {
            status: "unknown-member" as const,
            member: member.identity.name,
            ...progress,
          },
        };
      }
      const opened = yield* readNextBoundary(binding, member.through ?? member.from);
      if (opened.status === "missing" || opened.status === "gone") {
        return {
          kind: "result" as const,
          result: {
            status: opened.status,
            stream: "member" as const,
            member: member.identity.name,
          },
        };
      }
      if (opened.status === "idle") continue;

      const batch = opened.batch;
      const ack = yield* Effect.try({
        try: () => sourceAck(member.identity, batch.offset),
        catch: (cause) => new MalformedSourceBoundary({ offset: batch.offset, cause }),
      });
      const bytes = encodedBatchBytes(batch);
      const sizing = boundarySizing(options.limits, progress, bytes, "member");
      if (sizing) return { kind: "result" as const, result: { ...sizing, ...progress } };
      const boundary: ProjectionBoundary = { source: ack, page: progress.pages + 1, bytes };

      const inputs = yield* Effect.try({
        try: () => Array.from(options.decodeMember(batch, member, boundary)),
        catch: (cause) =>
          new ProjectionPoison({ phase: "member", sourcePosition: ack.position, cause }),
      });
      const itemLimit = itemSizing(options.limits, progress, inputs.length, "member");
      if (itemLimit) return { kind: "result" as const, result: { ...itemLimit, ...progress } };

      const stepped = yield* Effect.try({
        try: () => options.onRecord(progress.state, member, inputs, boundary),
        catch: (cause) =>
          new ProjectionPoison({ phase: "step", sourcePosition: ack.position, cause }),
      });
      const advancedMember: FanInMember = { ...member, through: ack.position };
      const appended = yield* appendFanInBatch(options, progress.checkpoint, {
        facts: stepped.facts,
        meta: [createMemberRow(advancedMember) as unknown as JsonValue],
        membershipThrough: progress.checkpoint.membershipThrough,
      });
      if (appended.kind === "failed") {
        return { kind: "result" as const, result: mergeFailure(appended.outcome, progress) };
      }
      return {
        kind: "advanced" as const,
        progress: {
          checkpoint: appended.checkpoint,
          members: sortMembers(
            progress.members.map((candidate) =>
              streamIdentityEquals(candidate.identity, member.identity)
                ? advancedMember
                : candidate,
            ),
          ),
          state: stepped.state,
          pages: progress.pages + 1,
          batches: progress.batches + 1,
          items: progress.items + inputs.length,
          bytes: progress.bytes + bytes,
        },
      };
    }
    return { kind: "idle" as const };
  });

interface FanInAppendInput {
  readonly facts: readonly JsonValue[];
  readonly meta: readonly JsonValue[];
  readonly membershipThrough: string | null;
}

/** Append outcomes that must be merged with caller progress before returning. */
type FanInAppendFailure =
  | { readonly status: "missing" | "gone"; readonly stream: "target" }
  | { readonly status: "output-conflict"; readonly reason: string; readonly offset?: string }
  | Extract<
      AppendDerivedStateResult,
      { readonly status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }
    >;

type FanInAppendStep =
  | { readonly kind: "committed"; readonly checkpoint: FanInCheckpoint }
  | { readonly kind: "failed"; readonly outcome: FanInAppendFailure };

interface FanInAppendLane {
  readonly lane: ProducerLane;
  readonly target: StreamBinding;
}

const appendFanInBatch = (
  options: FanInAppendLane,
  previous: FanInCheckpoint,
  input: FanInAppendInput,
): Effect.Effect<FanInAppendStep, MeshOperationalError, AppendStreams | FanInRecovery> =>
  Effect.gen(function* () {
    for (const fact of input.facts) {
      assertFactTypeAllowed(fact);
      assertStateFactShape(fact);
    }
    const nextProducerSeq = previous.nextProducerSeq + 1;
    if (!Number.isSafeInteger(nextProducerSeq)) throw new TypeError("Producer sequence exhausted");
    const checkpointRow = createFanInCheckpoint(options.lane, {
      membershipThrough: input.membershipThrough,
      nextProducerSeq,
    }) as unknown as JsonValue;

    const appends = yield* AppendStreams;
    const result = yield* appends.appendJsonBatch(
      options.target,
      [...input.facts, ...input.meta, checkpointRow],
      {
        expectedOffset: previous.targetOffset,
        producer: {
          producerId: options.lane.producerId,
          producerEpoch: options.lane.producerEpoch,
          producerSeq: previous.nextProducerSeq,
        },
      },
    );
    if (result.status === "appended") {
      return {
        kind: "committed" as const,
        checkpoint: {
          targetOffset: result.offset,
          membershipThrough: input.membershipThrough,
          nextProducerSeq,
          producerId: options.lane.producerId,
          producerEpoch: options.lane.producerEpoch,
        },
      };
    }
    if (result.status === "duplicate") {
      return yield* reconcileFanInDuplicate(options, previous, result, nextProducerSeq);
    }
    return { kind: "failed" as const, outcome: classifyFanInOutcome(result) };
  });

const reconcileFanInDuplicate = (
  options: FanInAppendLane,
  previous: FanInCheckpoint,
  duplicate: Extract<AppendOutcome, { status: "duplicate" }>,
  nextProducerSeq: number,
): Effect.Effect<FanInAppendStep, MeshOperationalError, FanInRecovery> =>
  Effect.gen(function* () {
    if (
      duplicate.producerEpoch !== options.lane.producerEpoch ||
      duplicate.producerSeq !== previous.nextProducerSeq
    ) {
      return yield* new IncompatibleLineage({
        message: "Duplicate producer high-water does not match the submitted fan-in batch sequence",
      });
    }
    const recovery = yield* FanInRecovery;
    const recovered = yield* recovery.recoverFanIn(options.target, options.lane);
    if (recovered.status !== "ready") {
      return {
        kind: "failed" as const,
        outcome: {
          status: recovered.status === "not-found" ? ("missing" as const) : ("gone" as const),
          stream: "target" as const,
        },
      };
    }
    if (
      recovered.checkpoint.targetOffset !== duplicate.offset ||
      recovered.checkpoint.nextProducerSeq !== nextProducerSeq
    ) {
      return yield* new IncompatibleLineage({
        message: "In-band fan-in checkpoint does not reconcile the accepted producer sequence",
      });
    }
    // Payload equality is never claimed; only the accepted sequence is reconciled.
    return { kind: "committed" as const, checkpoint: recovered.checkpoint };
  });

function classifyFanInOutcome(
  result: Exclude<AppendOutcome, { status: "appended" | "duplicate" }>,
): FanInAppendFailure {
  if (result.status === "not-found") return { status: "missing", stream: "target" };
  if (result.status === "gone") return { status: "gone", stream: "target" };
  if (result.status === "conflict") {
    return {
      status: "output-conflict",
      reason: result.conflictReason,
      ...("offset" in result ? { offset: result.offset } : {}),
    };
  }
  if (result.status === "closed") {
    return { status: "output-conflict", reason: "closed", offset: result.offset };
  }
  return result;
}

type BoundaryRead =
  | { readonly status: "ok"; readonly batch: StreamBatch }
  | { readonly status: "idle" }
  | { readonly status: "missing" }
  | { readonly status: "gone" };

/** Read exactly one payload-bearing boundary, then release the read session. */
const readNextBoundary = (
  binding: StreamBinding,
  after: string | null,
): Effect.Effect<BoundaryRead, MeshOperationalError, ReadStreams> =>
  Effect.gen(function* () {
    const reads = yield* ReadStreams;
    const opened = yield* reads.open(binding, {
      ...(after === null ? {} : { offset: after }),
      live: false,
    });
    if (opened.status !== "ok") {
      return {
        status: opened.status === "not-found" ? ("missing" as const) : ("gone" as const),
      } satisfies BoundaryRead;
    }
    while (true) {
      const next = yield* opened.session.next;
      if (next.done) {
        const ended = yield* opened.session.done;
        if (ended.status === "cancelled") return yield* Effect.interrupt;
        return { status: "idle" as const } satisfies BoundaryRead;
      }
      if (hasSourcePayload(next.value)) {
        return { status: "ok" as const, batch: next.value } satisfies BoundaryRead;
      }
    }
  }).pipe(Effect.scoped);

const makeFanInScan = (reads: ReadStreamsShape) =>
  Effect.fn("FanInRecovery.recoverFanIn")((target: StreamBinding, lane: ProducerLane) =>
    Effect.gen(function* () {
      assertLaneTarget(target, lane);
      const opened = yield* reads.open(target);
      if (opened.status !== "ok") return opened;
      const session = opened.session;
      if (session.startOffset === undefined) {
        return yield* new MalformedLineage({
          message: "Fan-in State recovery read did not provide a start offset",
          cause: opened,
        });
      }
      let targetOffset = session.startOffset;
      let membershipThrough: string | null = null;
      let nextProducerSeq = 0;
      let sawItems = false;
      let lastWasCheckpoint = false;
      const members = new Map<string, FanInMember>();
      const facts: JsonValue[] = [];

      const pull = Effect.gen(function* () {
        while (true) {
          const next = yield* session.next;
          if (next.done) break;
          const batch = next.value;
          targetOffset = batch.offset;
          if (batch.kind !== "json") {
            return yield* new IncompatibleLineage({ message: "Fan-in State target is not JSON" });
          }
          for (const item of batch.items) {
            sawItems = true;
            lastWasCheckpoint = false;
            if (!isRecord(item) || typeof item.type !== "string") {
              facts.push(item);
              continue;
            }
            if (item.type === FAN_IN_CHECKPOINT_TYPE) {
              const decoded = yield* decodeFanInCheckpoint(item);
              yield* ensureFanInCheckpointCompatible(decoded, lane);
              membershipThrough = decoded.value.membershipThrough;
              nextProducerSeq = decoded.value.nextProducerSeq;
              lastWasCheckpoint = true;
            } else if (item.type === FAN_IN_MEMBER_TYPE) {
              const operation = isRecord(item.headers) ? item.headers.operation : undefined;
              if (operation === "delete") {
                const key = typeof item.key === "string" ? item.key : "";
                if (!key.startsWith(MEMBER_KEY_PREFIX)) {
                  return yield* new MalformedLineage({
                    message: "Fan-in member removal row has an unusable key",
                    cause: item,
                  });
                }
                members.delete(key.slice(MEMBER_KEY_PREFIX.length));
                continue;
              }
              const decoded = yield* decodeFanInMember(item);
              const identity = yield* Effect.try({
                try: () => decodeStreamIdentity(decoded.value.memberIdentity),
                catch: (cause) =>
                  new MalformedLineage({ message: "Malformed fan-in member identity", cause }),
              });
              members.set(decoded.value.memberIdentity, {
                identity,
                from: decoded.value.from,
                through: decoded.value.through,
              });
            } else if (isReservedType(item.type)) {
              return yield* new IncompatibleLineage({
                message: `Unknown reserved State type ${item.type}`,
              });
            } else {
              facts.push(item);
            }
          }
        }
        const ended = yield* session.done;
        if (ended.status === "cancelled") return yield* Effect.interrupt;
        if (sawItems && !lastWasCheckpoint) {
          return yield* new IncompatibleLineage({
            message: "Fan-in State history does not end at a checkpoint transaction boundary",
          });
        }
        return {
          status: "ready" as const,
          checkpoint: {
            targetOffset,
            membershipThrough,
            nextProducerSeq,
            producerId: lane.producerId,
            producerEpoch: lane.producerEpoch,
          },
          members: sortMembers(Array.from(members.values())),
          facts: facts as readonly JsonValue[],
        };
      });
      return yield* pull;
    }).pipe(Effect.scoped),
  );

function mergeFailure<State>(
  outcome: FanInAppendFailure,
  progress: CatchUpFanInProgress<State>,
): CatchUpFanInResult<State> {
  if (outcome.status === "missing" || outcome.status === "gone") return outcome;
  return { ...outcome, ...progress };
}

function sortMembers(members: readonly FanInMember[]): readonly FanInMember[] {
  return members.toSorted(compareMembers);
}

function boundarySizing<State>(
  limits: CatchUpLimits,
  progress: CatchUpFanInProgress<State>,
  bytes: number,
  stream: "membership" | "member",
) {
  if (bytes > limits.maxBytes) {
    return {
      status: "boundary-too-large" as const,
      limit: "maxBytes" as const,
      stream,
      actual: bytes,
      maximum: limits.maxBytes,
    };
  }
  if (progress.bytes + bytes > limits.maxBytes) {
    return { status: "limit-reached" as const, limit: "maxBytes" as const };
  }
  return undefined;
}

function itemSizing<State>(
  limits: CatchUpLimits,
  progress: CatchUpFanInProgress<State>,
  count: number,
  stream: "membership" | "member",
) {
  if (count > limits.maxItems) {
    return {
      status: "boundary-too-large" as const,
      limit: "maxItems" as const,
      stream,
      actual: count,
      maximum: limits.maxItems,
    };
  }
  if (progress.items + count > limits.maxItems) {
    return { status: "limit-reached" as const, limit: "maxItems" as const };
  }
  return undefined;
}

function validateFanInOptions<State, MemberInput>(
  options: CatchUpFanInStateOptions<State, MemberInput>,
): void {
  if (!streamIdentityEquals(options.membership.identity, options.lane.source))
    throw new TypeError("Membership binding identity does not match the producer lane source");
  assertLaneTarget(options.target, options.lane);
  for (const [name, value] of Object.entries(options.limits)) {
    if (!Number.isSafeInteger(value) || value <= 0)
      throw new TypeError(`${name} must be a positive safe integer`);
  }
}

function assertLaneTarget(target: StreamBinding, lane: ProducerLane): void {
  if (!streamIdentityEquals(target.identity, lane.target))
    throw new TypeError("Target binding identity does not match the producer lane");
}

function hasSourcePayload(batch: StreamBatch): boolean {
  if (batch.kind === "json") return batch.items.length > 0;
  if (batch.kind === "text") return batch.text.length > 0;
  return batch.data.byteLength > 0;
}

function encodedBatchBytes(batch: StreamBatch): number {
  if (batch.kind === "json")
    return new TextEncoder().encode(JSON.stringify(batch.items)).byteLength;
  if (batch.kind === "text") return new TextEncoder().encode(batch.text).byteLength;
  return batch.data.byteLength;
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
