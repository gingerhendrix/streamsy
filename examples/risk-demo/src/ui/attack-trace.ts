/**
 * The newest resolved throw, as the *map* needs to see it.
 *
 * The dice card explains a throw to the player who made it. The map answers a
 * different question — *where did that just happen, and what did it cost?* — and it
 * is the only surface a spectator of an agent-versus-agent match is watching, so a
 * throw that leaves no mark on the map effectively did not happen for them.
 *
 * Nothing new is recorded to make this possible. `AttackResolved` already carries
 * the route and both losses into the bounded `moves` feed, so the overlay is a pure
 * read of state the board projection already holds. The newest throw is always
 * inside that window: the feed keeps 40 rows and a single throw is two of them.
 *
 * Ordering follows the feed's own rule (`boardRowsFromQueries` sorts moves by
 * `sourceOffset`), but the comparison is made here rather than assumed, so the
 * derivation is correct whichever way round the caller holds the feed.
 */

import type { ProjectedMove } from "../board/projection.ts";

/** One resolved throw, reduced to what the map draws. */
export interface AttackTrace {
  /** Unique per throw — "attack again" declares a new attack — so it keys the fade. */
  attackId: string;
  from: string;
  to: string;
  attackerLosses: number;
  defenderLosses: number;
  /** A capture is drawn differently from a bounce: the target changed hands. */
  captured: boolean;
  /**
   * Where this throw sits in canonical history — the source stream offset of its
   * `AttackResolved` event. Carried so "did this happen before I opened the screen?"
   * can be answered by comparing two offsets from the same total order rather than
   * by guessing from what has arrived.
   */
  sourceOffset: string;
}

export function latestAttackTrace(moves: readonly ProjectedMove[]): AttackTrace | null {
  let newest: ProjectedMove | undefined;
  for (const move of moves) {
    if (move.kind !== "AttackResolved") continue;
    // A row missing its route cannot be drawn; skipping it is better than guessing
    // at coordinates the projection did not record.
    if (move.attackId === undefined || move.from === undefined || move.to === undefined) continue;
    if (!newest || move.sourceOffset.localeCompare(newest.sourceOffset) > 0) newest = move;
  }
  if (!newest) return null;
  return {
    attackId: newest.attackId!,
    from: newest.from!,
    to: newest.to!,
    attackerLosses: newest.attackerLosses ?? 0,
    defenderLosses: newest.defenderLosses ?? 0,
    captured: newest.territoryCaptured === true,
    sourceOffset: newest.sourceOffset,
  };
}

/**
 * The trace to actually draw, given how far canonical history had run when this
 * screen opened.
 *
 * A throw that resolved before the viewer arrived is state, not an event they are
 * watching, so it must not fade in front of them on load. Every earlier cut of this
 * gate asked *"is the newest throw I can see the same one I could see a moment ago?"*
 * and every one of them leaked, because what a freshly loaded page "can see a moment
 * ago" is not canonical history — the projection stream's first response is served
 * `Cache-Control: public, max-age=60, stale-while-revalidate=300`, so the browser
 * hydrates from an HTTP-cached snapshot that is internally coherent (its meta row,
 * its move rows and its watermark all agree) but up to a minute stale, and *claims*
 * `stream-up-to-date: true`. Everything committed since then is then delivered as
 * ordinary live changes, indistinguishable from news. Naming history from the first
 * state to arrive — a truthy `board`, then a non-null `meta` row — could only ever
 * name the cached one.
 *
 * So the comparison is not between two views of the feed but between two points in
 * one total order: the throw's own `sourceOffset`, and the watermark canonical
 * history stood at when the screen opened, read from an authoritative resource that
 * no cache answers for. A throw at or below that watermark provably already existed;
 * a throw above it provably did not. Which batch delivered it, and whether that
 * batch came from a cache, a catch-up or a long poll, stops mattering.
 *
 * `undefined` means *the watermark is not known yet*, which is deliberately
 * different from `null` ("history was empty when we opened"): until it is known,
 * nothing may be drawn, because a missed flash costs less than a false one. The
 * comparison is re-made on every render rather than latched, so a throw that lands
 * during that window is drawn as soon as the watermark proves it is new.
 *
 * The *watermark itself* must be fixed at open, though — see `openedThroughWatermark`
 * for why a watermark that keeps up with the feed suppresses everything forever.
 */
export function traceToDraw(
  trace: AttackTrace | null,
  openedThroughOffset: string | null | undefined,
): AttackTrace | null {
  if (!trace) return null;
  if (openedThroughOffset === undefined) return null;
  if (openedThroughOffset === null) return trace;
  // Offsets are fixed-width, so the feed's own ordering rule compares them.
  return trace.sourceOffset.localeCompare(openedThroughOffset) <= 0 ? null : trace;
}

/** How the authoritative `GET /board` watermark read is getting on. */
export type WatermarkRead =
  | { state: "reading" }
  | { state: "known"; offset: string | null }
  | { state: "failed" };

/**
 * Where the fallback watermark is remembered across renders. One per screen open;
 * `offset` is written at most once.
 */
export interface WatermarkLatch {
  offset?: string;
}

/**
 * The watermark to gate on, resolving the authoritative read against the projection's
 * own watermark when that read fails.
 *
 * The fallback has to be **latched**, and this is the whole substance of the function.
 * `meta.sourceThroughOffset` is not a fixed point: it tracks the newest move in the
 * feed, render for render. Handing it straight to `traceToDraw` therefore compares
 * every throw against a watermark that has already moved past it, so *nothing is ever
 * new* — the overlay stops drawing for the rest of the session, including throws that
 * resolve minutes later while the viewer is watching. Measured, not theorised: with
 * the read blocked, 51 s of live play across several resolved throws drew nothing at
 * all. Silently deleting the feature is worse than the defect it prevents, so the
 * first value the fallback offers is kept and the later ones ignored.
 *
 * Latched, the failed path degrades to the guarantee the identity-based gate gave
 * before the watermark existed: the newest throw already on screen when the read
 * failed may still flash once, because the state carrying it can itself be the stale
 * cached hydration — but every throw after it is correctly news. A once-per-load
 * false flash on a path that needs `/board` to fail is the accepted trade; silence
 * is not.
 *
 * Returns `undefined` while nothing can be said yet — either the read is in flight,
 * or it failed before the projection had produced a watermark to fall back on.
 */
export function openedThroughWatermark(
  read: WatermarkRead,
  fallbackOffset: string | undefined,
  latch: WatermarkLatch,
): string | null | undefined {
  if (read.state === "known") return read.offset;
  if (read.state === "reading") return undefined;
  latch.offset ??= fallbackOffset;
  return latch.offset;
}
