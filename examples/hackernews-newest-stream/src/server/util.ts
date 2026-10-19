import { Clock, DateTime, Effect } from "effect";

export function errorMessage(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

export const nowIso = Effect.map(Clock.currentTimeMillis, (millis) =>
  DateTime.formatIso(DateTime.makeUnsafe(millis)),
);
