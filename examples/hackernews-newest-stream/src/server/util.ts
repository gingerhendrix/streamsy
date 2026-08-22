import { Clock, DateTime, Effect } from "effect";

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export const nowIso = Effect.map(Clock.currentTimeMillis, (millis) =>
  DateTime.formatIso(DateTime.makeUnsafe(millis)),
);
