import { Data, DateTime, Predicate } from "effect";

/** A process handler's request to run again at an epoch-millisecond deadline. */
export interface WakeAt {
  readonly _tag: "WakeAt";
  readonly wakeAt: number;
}

const request = Data.taggedEnum<WakeAt>().WakeAt;

/** Return this request from process; returning void clears the previous deadline. */
export const wakeAt = (time: number | DateTime.DateTime): WakeAt => {
  const millis = Predicate.isNumber(time) ? time : DateTime.toEpochMillis(time);

  if (!Number.isSafeInteger(millis) || millis < 0)
    throw new RangeError("wakeAt must be a non-negative safe integer in epoch milliseconds");

  return request({ wakeAt: millis });
};
