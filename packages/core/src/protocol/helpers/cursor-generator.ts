/**
 * Live-read cursor generation for the durable streams protocol.
 *
 * Responsibilities:
 *
 * - The current interval is `floor((clock.now() - CURSOR_EPOCH_MS) / CURSOR_INTERVAL_MS)`,
 *   with epoch `2024-10-09T00:00:00.000Z` and a 20s interval.
 * - With no previous cursor, the result is `String(currentInterval)`.
 * - With a previous cursor strictly below the current interval, the result is
 *   `String(currentInterval)`.
 * - With a previous cursor at or above the current interval, the result is
 *   `String(previousInterval + Math.max(1, Math.floor(random() * 180)))` so
 *   the cursor still advances even when no real interval has elapsed.
 * - The previous cursor is parsed with `parseInt(previous, 10)`, preserving
 *   lenient parsing for unexpected cursor strings.
 * - `random` defaults to `Math.random` and may be injected for tests.
 *
 * Used by `LiveReadService` when shaping every live-read response cursor.
 */
import type { Clock } from "../../types/storage.ts";

export const CURSOR_EPOCH_MS = new Date("2024-10-09T00:00:00.000Z").getTime();
export const CURSOR_INTERVAL_MS = 20_000;

export function generateCursor(
  clock: Clock,
  previous?: string,
  random: () => number = Math.random,
): string {
  const currentInterval = Math.floor((clock.now() - CURSOR_EPOCH_MS) / CURSOR_INTERVAL_MS);
  if (!previous) return String(currentInterval);
  const previousInterval = parseInt(previous, 10);
  if (previousInterval < currentInterval) return String(currentInterval);
  return String(previousInterval + Math.max(1, Math.floor(random() * 180)));
}
