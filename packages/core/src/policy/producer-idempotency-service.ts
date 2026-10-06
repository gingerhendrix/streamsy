/** Pure producer idempotency decisions. */

import { Data, Match } from "effect";
import { StaleEpoch, ProducerGap, InvalidEpochSeq } from "../protocol/errors.ts";
import type { StreamId } from "../schema/index.ts";
import type { ProducerState } from "../schema/index.ts";

export type ProducerValidation = Data.TaggedEnum<{
  Accepted: { readonly proposedState: ProducerState };
  Duplicate: { readonly lastSeq: number; readonly epoch: number };
  StaleEpoch: { readonly currentEpoch: number };
  Gap: { readonly expectedSeq: number; readonly receivedSeq: number };
  InvalidEpochSeq: {};
}>;

export const ProducerValidation = Data.taggedEnum<ProducerValidation>();

export type ProducerRejection = Exclude<ProducerValidation, { _tag: "Accepted" }>;

export function validateProducer(
  state: ProducerState | undefined,
  epoch: number,
  seq: number,
): ProducerValidation {
  if (!state)
    return seq === 0
      ? ProducerValidation.Accepted({ proposedState: { epoch, lastSeq: 0 } })
      : ProducerValidation.Gap({ expectedSeq: 0, receivedSeq: seq });

  if (epoch < state.epoch) return ProducerValidation.StaleEpoch({ currentEpoch: state.epoch });

  if (epoch > state.epoch)
    return seq === 0
      ? ProducerValidation.Accepted({ proposedState: { epoch, lastSeq: 0 } })
      : ProducerValidation.InvalidEpochSeq();

  if (seq <= state.lastSeq)
    return ProducerValidation.Duplicate({ lastSeq: state.lastSeq, epoch: state.epoch });

  if (seq === state.lastSeq + 1)
    return ProducerValidation.Accepted({ proposedState: { epoch, lastSeq: seq } });

  return ProducerValidation.Gap({ expectedSeq: state.lastSeq + 1, receivedSeq: seq });
}

export function rejectionToAppendError(
  rejection: Exclude<ProducerRejection, { _tag: "Duplicate" }>,
  id: StreamId,
): StaleEpoch | ProducerGap | InvalidEpochSeq {
  return Match.valueTags(rejection, {
    StaleEpoch: ({ currentEpoch }) => new StaleEpoch({ id, currentEpoch }),
    Gap: ({ expectedSeq, receivedSeq }) => new ProducerGap({ id, expectedSeq, receivedSeq }),
    InvalidEpochSeq: () => new InvalidEpochSeq({ id }),
  });
}
