/**
 * Caller-local cancellation for the live-wait seam.
 *
 * Races a storage `awaitChange` against a caller-local `AbortSignal`. The storage
 * wait is never told about the signal — keeping all DOM/platform types off the
 * storage seam (the same constraint that removed `AbortSignal` from the seam in
 * the first place). If the signal wins, the storage wait is left to finish
 * naturally by its own `timeoutMs` (bounded; the Durable Object caps it). We
 * resolve to a timeout-shaped result so the caller's normal "no new data" path
 * runs and the SSE loop tears down.
 */
import type { AwaitChangeResult, StreamChangeSnapshot } from "../../types/storage.ts";

export function raceAbortAwaitChange(
  wait: Promise<AwaitChangeResult>,
  fallbackSnapshot: StreamChangeSnapshot,
  signal?: AbortSignal,
): Promise<AwaitChangeResult> {
  if (!signal) return wait;

  const timeoutResult: AwaitChangeResult = { status: "timeout", snapshot: fallbackSnapshot };
  if (signal.aborted) return Promise.resolve(timeoutResult);

  return new Promise<AwaitChangeResult>((resolve, reject) => {
    const onAbort = () => {
      cleanup();
      resolve(timeoutResult);
    };
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    signal.addEventListener("abort", onAbort, { once: true });
    wait.then(
      (result) => {
        cleanup();
        resolve(result);
      },
      (error) => {
        cleanup();
        reject(error);
      },
    );
  });
}
