/**
 * Optional dependency lookup helpers for a composed {@link Stream}.
 *
 * Optional behaviour on a stream — producer state, fork reference counts,
 * mutation serialization, live-read events, active expiry — is surfaced as
 * additional members on the returned `Stream`. Adapters that do not support
 * a behaviour simply omit the corresponding member.
 *
 * Protocol code that covers optional behaviour must turn a missing member
 * into a structured {@link NotSupportedResult} rather than throwing or
 * accidentally no-opping. These helpers centralise that lookup so callers
 * can write:
 *
 * ```ts
 * const producers = requireProducerStore(stream);
 * if (isNotSupported(producers)) return producers;
 * await producers.setProducerState(producerId, state);
 * ```
 *
 * Helper names mirror the feature ids used in HTTP responses so the
 * machine-readable feature stays consistent across protocol and transport.
 */
import type {
  NotSupportedResult,
  Stream,
  StreamEventHub,
  StreamExpiryScheduler,
  StreamMutationCoordinator,
  StreamProducerStore,
  StreamReferenceTracker,
} from "../types/factory.ts";
import { notSupported } from "../types/factory.ts";

export function requireProducerStore(
  stream: Stream,
  message?: string,
): StreamProducerStore | NotSupportedResult {
  return stream.producers ?? notSupported("producer-idempotency", message);
}

export function requireReferenceTracker(
  stream: Stream,
  message?: string,
): StreamReferenceTracker | NotSupportedResult {
  return stream.references ?? notSupported("fork", message);
}

export function requireMutationCoordinator(
  stream: Stream,
  message?: string,
): StreamMutationCoordinator | NotSupportedResult {
  return stream.mutations ?? notSupported("mutation-lock", message);
}

export function requireEventHub(
  stream: Stream,
  message?: string,
): StreamEventHub | NotSupportedResult {
  return stream.events ?? notSupported("live-read", message);
}

export function requireExpiryScheduler(
  stream: Stream,
  message?: string,
): StreamExpiryScheduler | NotSupportedResult {
  return stream.expiry ?? notSupported("active-expiry", message);
}
