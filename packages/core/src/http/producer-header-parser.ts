import type { ProducerOptions } from "../types/protocol.ts";

const MAX_SAFE_PRODUCER_INT = Number.MAX_SAFE_INTEGER;
const NON_NEGATIVE_INT_RE = /^(0|[1-9]\d*)$/;

export type ProducerHeaderResult =
  | { kind: "absent" }
  | { kind: "ok"; producer: ProducerOptions }
  | { kind: "invalid" };

export class ProducerHeaderParser {
  parse(request: Request): ProducerHeaderResult {
    const id = request.headers.get("producer-id");
    const epoch = request.headers.get("producer-epoch");
    const seq = request.headers.get("producer-seq");

    const present = [id, epoch, seq].filter((v) => v !== null).length;
    if (present === 0) return { kind: "absent" };
    if (present !== 3) return { kind: "invalid" };
    if (id!.length === 0) return { kind: "invalid" };

    const parsedEpoch = this.parseInt(epoch!);
    const parsedSeq = this.parseInt(seq!);
    if (parsedEpoch === null || parsedSeq === null) return { kind: "invalid" };

    return {
      kind: "ok",
      producer: { producerId: id!, producerEpoch: parsedEpoch, producerSeq: parsedSeq },
    };
  }

  private parseInt(raw: string): number | null {
    if (!NON_NEGATIVE_INT_RE.test(raw)) return null;
    const n = Number(raw);
    if (!Number.isFinite(n) || n > MAX_SAFE_PRODUCER_INT) return null;
    return n;
  }
}
