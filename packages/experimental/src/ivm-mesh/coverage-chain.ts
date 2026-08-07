import { coverage, type SourceAck, type SourceWatermark } from "../causal.ts";

/**
 * One durable hop in a fixed derivation chain.
 *
 * `watermark` is the direct-source position this output has incorporated.
 * `output` is the output's own durable position, which becomes the
 * acknowledgement compared against the next hop.
 */
export interface ChainHop {
  readonly label: string;
  readonly watermark?: SourceWatermark;
  readonly output?: SourceAck;
}

export interface ProvenHop {
  readonly label: string;
  readonly watermark: SourceWatermark;
  readonly output: SourceAck;
}

export type ChainedCoverage =
  | { readonly status: "proven"; readonly hops: readonly ProvenHop[] }
  | {
      readonly status: "not-yet";
      readonly blockedAt: string;
      readonly hops: readonly ProvenHop[];
    }
  | {
      readonly status: "incomparable";
      readonly blockedAt: string;
      readonly hops: readonly ProvenHop[];
    };

/**
 * Prove a fixed derivation chain by reading direct-source lineage at each
 * output. This helper is deliberately bounded to an ordered, known path; a
 * generic causal graph query is deferred.
 *
 * A wake receipt or elapsed delay can never produce `proven`.
 */
export function chainedCoverage(ack: SourceAck, hops: readonly ChainHop[]): ChainedCoverage {
  const proven: ProvenHop[] = [];
  let current = ack;
  for (const hop of hops) {
    if (hop.watermark === undefined || hop.output === undefined) {
      return { status: "not-yet", blockedAt: hop.label, hops: proven };
    }
    const result = coverage(hop.watermark, current);
    if (result.status === "incomparable") {
      return { status: "incomparable", blockedAt: hop.label, hops: proven };
    }
    if (result.status === "not-yet") {
      return { status: "not-yet", blockedAt: hop.label, hops: proven };
    }
    proven.push({ label: hop.label, watermark: hop.watermark, output: hop.output });
    current = hop.output;
  }
  return { status: "proven", hops: proven };
}
