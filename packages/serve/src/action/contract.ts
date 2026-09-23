import { contractFingerprint } from "../contract.ts";
/**
 * The action-sink declaration.
 *
 * A `stateSink` publishes a relation as data a consumer reads. An `actionSink`
 * does the opposite: it turns changes to a relation into *external effects* —
 * a notification, a webhook, a mail — which the world observes once and cannot
 * un-observe. That asymmetry is the whole design. A published row can be
 * republished; a delivered effect cannot be undelivered, so the contract is
 * built around a durable outbox, a deterministic idempotency key, a bounded
 * serialized retry policy, and a dead-letter terminus.
 *
 * The declaration is inert data. It names the relation it observes, the handler
 * that performs the effect, the payload's wire boundary, and the delivery
 * policy. It contains no I/O and no runtime state.
 */

/** The part of a keyed relation an action sink needs: the key its rows are declared by. */
export interface ActionSinkRelation<Key extends string = string> {
  readonly key: Key;
}

/**
 * One change to the observed relation.
 *
 * This is structurally the engine's `Change`, restated here so the declaration
 * surface does not drag the IR package into every consumer of a sink contract.
 */
export type ActionSinkChange<Row, Key = string> =
  | { readonly kind: "enter"; readonly key: Key; readonly after: Row }
  | { readonly kind: "update"; readonly key: Key; readonly before: Row; readonly after: Row }
  | { readonly kind: "exit"; readonly key: Key; readonly before: Row };

/** The handler an action sink is bound to, by name and version. */
export interface ActionSinkHandlerRef {
  readonly name: string;
  readonly version: number;
}

/**
 * The bounded retry policy.
 *
 * `maxAttempts` counts *total* attempts, so `maxAttempts: 1` means deliver once
 * and dead-letter on failure. Backoff is exponential and clamped, and it is
 * computed rather than scheduled: the outbox stores the next attempt instant,
 * so a restart resumes the same policy instead of restarting it.
 */
export interface ActionSinkDeliveryPolicy {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly backoffFactor: number;
  readonly maxBackoffMs: number;
}

/** The durable payload's wire boundary: JSON text in the outbox, a typed value in the handler. */
export interface ActionSinkPayloadCodec<Payload> {
  readonly encode: (payload: Payload) => string;
  /* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This decoder IS the outbox payload's parse boundary; a stored payload is untrusted JSON until it runs. */
  readonly decode: (value: unknown) => Payload;
}

export interface ActionSinkSpec<Payload, From extends ActionSinkRelation = ActionSinkRelation> {
  readonly name: string;
  readonly from: From;
  readonly handler: ActionSinkHandlerRef;
  readonly payload: ActionSinkPayloadCodec<Payload>;
  /**
   * The delivery's durable identity.
   *
   * It must be a pure function of durable facts, because it is the only thing
   * standing between a retried enqueue and a second real-world effect. Two
   * enqueues that describe the same fact must produce the same key.
   */
  readonly idempotencyKey: (payload: Payload) => string;
  /** Which serialized lane the delivery belongs to. Usually the relation's partition. */
  readonly partitionBy: (payload: Payload) => string;
  readonly delivery: ActionSinkDeliveryPolicy;
}

export interface CheckedActionSink<
  Payload,
  From extends ActionSinkRelation = ActionSinkRelation,
> extends ActionSinkSpec<Payload, From> {
  readonly kind: "checked-action-sink";
  /** The observed relation's declared key, carried so consumers read one key, not two. */
  readonly key: From["key"];
  /**
   * The contract's identity.
   *
   * It covers the sink name, the relation key, the handler reference and the
   * delivery policy — every part a consumer or an operator can observe. It
   * deliberately does not cover the declared functions: a closure has no honest
   * hash, and pretending otherwise would make the fingerprint a decoration.
   */
  readonly fingerprint: string;
}

export function defineActionSink<Payload, From extends ActionSinkRelation>(
  spec: ActionSinkSpec<Payload, From>,
): CheckedActionSink<Payload, From> {
  if (!Number.isInteger(spec.delivery.maxAttempts) || spec.delivery.maxAttempts < 1) {
    throw new TypeError(`action sink ${spec.name} must declare at least one delivery attempt`);
  }
  if (spec.delivery.backoffFactor < 1) {
    throw new TypeError(`action sink ${spec.name} must not shrink its backoff`);
  }
  if (
    spec.delivery.initialBackoffMs < 0 ||
    spec.delivery.maxBackoffMs < spec.delivery.initialBackoffMs
  ) {
    throw new TypeError(`action sink ${spec.name} declares an impossible backoff window`);
  }
  const key = spec.from.key;
  const fingerprint = contractFingerprint({
    name: spec.name,
    key,
    handler: { ...spec.handler },
    delivery: Object.fromEntries(
      Object.entries(spec.delivery).map(([name, value]) => [
        name,
        Number.isFinite(value) ? value : String(value),
      ]),
    ),
  });
  return Object.freeze({ ...spec, kind: "checked-action-sink", key, fingerprint });
}

/** The next backoff, in milliseconds, after `attempts` failed attempts. */
export function backoffAfter(policy: ActionSinkDeliveryPolicy, attempts: number): number {
  const exponent = Math.max(0, attempts - 1);
  const raw = policy.initialBackoffMs * policy.backoffFactor ** exponent;
  return Math.min(policy.maxBackoffMs, Math.round(raw));
}

export type PayloadOf<Sink> =
  Sink extends CheckedActionSink<infer Payload, infer _From> ? Payload : never;
