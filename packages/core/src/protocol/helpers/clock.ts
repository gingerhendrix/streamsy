/**
 * Default `Clock` implementation backed by the host runtime.
 *
 * Responsibilities:
 *
 * - `now()` returns `Date.now()`.
 * - `date(value)` returns `new Date(value ?? Date.now())`, so calling
 *   `systemClock.date()` with no argument yields the current moment, while
 *   passing a number or string delegates to the standard `Date` constructor.
 *
 * Used as the fallback when `StreamProtocolOptions.clock` is omitted; tests
 * may inject a custom `Clock` to make time deterministic.
 */
import type { Clock } from "../../types/storage.ts";

export const systemClock: Clock = {
  now: () => Date.now(),
  date: (value?: number | string) => new Date(value ?? Date.now()),
};
