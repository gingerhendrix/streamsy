/**
 * The effect-sink declaration.
 *
 * A `stateSink` publishes a relation as data a consumer reads. An `effectSink`
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

/** The part of a keyed relation an effect sink needs: the key its rows are declared by. */
export interface EffectSinkRelation<Key extends string = string> {
  readonly key: Key;
}

/**
 * One change to the observed relation.
 *
 * This is structurally the engine's `Change`, restated here so the declaration
 * surface does not drag the IR package into every consumer of a sink contract.
 */
export type EffectSinkChange<Row, Key = string> =
  | { readonly kind: "enter"; readonly key: Key; readonly after: Row }
  | { readonly kind: "update"; readonly key: Key; readonly before: Row; readonly after: Row }
  | { readonly kind: "exit"; readonly key: Key; readonly before: Row };

/** The handler an effect sink is bound to, by name and version. */
export interface EffectSinkHandlerRef {
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
export interface EffectSinkDeliveryPolicy {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly backoffFactor: number;
  readonly maxBackoffMs: number;
}

/** The durable payload's wire boundary: JSON text in the outbox, a typed value in the handler. */
export interface EffectSinkPayloadCodec<Payload> {
  readonly encode: (payload: Payload) => string;
  /* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This decoder IS the outbox payload's parse boundary; a stored payload is untrusted JSON until it runs. */
  readonly decode: (value: unknown) => Payload;
}

export interface EffectSinkSpec<Payload, From extends EffectSinkRelation = EffectSinkRelation> {
  readonly name: string;
  readonly from: From;
  readonly handler: EffectSinkHandlerRef;
  readonly payload: EffectSinkPayloadCodec<Payload>;
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
  readonly delivery: EffectSinkDeliveryPolicy;
}

export interface CheckedEffectSink<
  Payload,
  From extends EffectSinkRelation = EffectSinkRelation,
> extends EffectSinkSpec<Payload, From> {
  readonly kind: "checked-effect-sink";
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

export function defineEffectSink<Payload, From extends EffectSinkRelation>(
  spec: EffectSinkSpec<Payload, From>,
): CheckedEffectSink<Payload, From> {
  if (!Number.isInteger(spec.delivery.maxAttempts) || spec.delivery.maxAttempts < 1) {
    throw new TypeError(`effect sink ${spec.name} must declare at least one delivery attempt`);
  }
  if (spec.delivery.backoffFactor < 1) {
    throw new TypeError(`effect sink ${spec.name} must not shrink its backoff`);
  }
  if (
    spec.delivery.initialBackoffMs < 0 ||
    spec.delivery.maxBackoffMs < spec.delivery.initialBackoffMs
  ) {
    throw new TypeError(`effect sink ${spec.name} declares an impossible backoff window`);
  }
  const key = spec.from.key;
  const fingerprint = hashContract({
    name: spec.name,
    key,
    handler: spec.handler,
    delivery: spec.delivery,
  });
  return Object.freeze({ ...spec, kind: "checked-effect-sink", key, fingerprint });
}

/** The next backoff, in milliseconds, after `attempts` failed attempts. */
export function backoffAfter(policy: EffectSinkDeliveryPolicy, attempts: number): number {
  const exponent = Math.max(0, attempts - 1);
  const raw = policy.initialBackoffMs * policy.backoffFactor ** exponent;
  return Math.min(policy.maxBackoffMs, Math.round(raw));
}

interface ContractFingerprintInput {
  readonly name: string;
  readonly key: string;
  readonly handler: EffectSinkHandlerRef;
  readonly delivery: EffectSinkDeliveryPolicy;
}

/** The same FNV-1a contract hash the checked state sink uses, so both read alike. */
function hashContract(value: ContractFingerprintInput): string {
  const input = JSON.stringify(value);
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export type PayloadOf<Sink> =
  Sink extends CheckedEffectSink<infer Payload, infer _From> ? Payload : never;
