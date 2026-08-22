import { encodeStreamIdentity, type StreamIdentity } from "../causal.ts";

const LANE_PREFIX = "streamsy-mesh-v1-";
export const MAX_PRODUCER_ID_LENGTH = LANE_PREFIX.length + 64;

export interface ProducerLaneConfig {
  readonly processorId: string;
  readonly processorVersion: string;
  readonly outputGeneration: string;
  readonly source: StreamIdentity;
  readonly target: StreamIdentity;
  /** Fixed for the immutable output generation. Restarts must reuse it. */
  readonly producerEpoch: number;
}

export interface ProducerLane extends ProducerLaneConfig {
  readonly producerId: string;
}

/** Derive one bounded producer id for the complete processor/generation/source/target lane. */
// oxlint-disable-next-line effecttsgo/async-function -- Web Crypto exposes digest as a Promise, and this exported lane helper preserves its public Promise contract.
export async function deriveProducerLane(config: ProducerLaneConfig): Promise<ProducerLane> {
  const canonical = canonicalLaneInput(config);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonical));
  const hex = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
  return Object.freeze({ ...validatedConfig(config), producerId: `${LANE_PREFIX}${hex}` });
}

export function canonicalLaneInput(config: ProducerLaneConfig): string {
  const valid = validatedConfig(config);
  return JSON.stringify([
    "streamsy.mesh.producer-lane.v1",
    valid.processorId,
    valid.processorVersion,
    valid.outputGeneration,
    encodeStreamIdentity(valid.source),
    encodeStreamIdentity(valid.target),
  ]);
}

function validatedConfig(config: ProducerLaneConfig): ProducerLaneConfig {
  return Object.freeze({
    processorId: requiredText(config.processorId, "processorId"),
    processorVersion: requiredText(config.processorVersion, "processorVersion"),
    outputGeneration: requiredText(config.outputGeneration, "outputGeneration"),
    source: config.source,
    target: config.target,
    producerEpoch: nonNegativeSafeInteger(config.producerEpoch, "producerEpoch"),
  });
}

function requiredText(value: string, name: string): string {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} is required`);
  if (value.length > 512) throw new TypeError(`${name} must not exceed 512 code units`);
  return value.normalize("NFC");
}

function nonNegativeSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}
