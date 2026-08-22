/* oxlint-disable effecttsgo/async-function -- This executable script is a bounded Promise-native Bun/Node adapter over the demo's public HTTP and application APIs. */
/* oxlint-disable effecttsgo/global-timers -- The public Promise bot facade exposes an abortable delay whose timer is owned and cleared by the caller-facing adapter. */
/**
 * Scripted-bot harness: plays Risk using only the published HTTP resources and
 * its per-player action stream — never the kernel directly.
 *
 * Loop: follow the self-sufficient action stream from a persisted cursor → choose
 * from `legalMoves` → POST one command → repeat.
 *
 * Restart-safe idempotency: a command's `commandId` is derived deterministically
 * from the observed board state (`playerId:turnId:<state fingerprint>`). After a
 * crash + resume from the saved cursor, re-deriving at the same board yields the
 * same id (idempotent retry); once a command commits the board changes, so the
 * next id differs.
 *
 * Crash-resume: the durable cursor never advances past a message whose command
 * has not been acknowledged. Advancing at *read* time is a livelock — a crash in
 * that window leaves the still-required action behind a cursor the consumer will
 * never rewind to, and the server has nothing new to emit because nothing new
 * has happened. So the persisted state carries two things: `cursor`, which
 * advances only on a terminal outcome, and `inflight`, the exact serialized body
 * of a command that was posted but never seen acked, replayed byte-identically
 * on restart so a commit that did land collapses to `duplicate`.
 *
 * An out-of-turn `roll-defense` uses the
 * stable id `bot-defense:<attackId>` rather than a board fingerprint, because
 * the board has not changed and the point is that a duplicate wake, a retry, and
 * a race with the canonical timeout must all collapse to one roll. Duplicate and
 * stale outcomes are treated as success — if the bot is offline entirely, the
 * canonical timeout resolves the combat without it.
 */

import { readActionsBatches } from "../../src/application/actions-stream.ts";
import type { ActionRequired, AgentMessage } from "../game/action-notifier.ts";
import {
  chooseAttack,
  chooseFortify,
  chooseOccupy,
  chooseReinforce,
  strategyContext,
  type StrategyMap,
} from "./strategy.ts";

export type HttpCall = (
  method: string,
  path: string,
  opts?: { token?: string; body?: unknown; accept?: string },
) => Promise<{ status: number; body: any }>;

/**
 * Open the actions SSE stream. Optional: a bot given one blocks on the stream,
 * and a bot without one reads the immediate JSON page each time it is asked.
 */
export type OpenActionsStream = (
  path: string,
  opts: { token: string; signal: AbortSignal },
) => Promise<Response>;

/** Persisted bot state: an answered-through cursor plus any unacked command. */
export interface BotState {
  /** Advances only once the message it points past has been answered. */
  cursor?: string;
  /** A command posted but not yet known-terminal; replayed verbatim on restart. */
  inflight?: { body: string; cursorAfter?: string };
}

export interface CreateBotOptions {
  call: HttpCall;
  /** Supply to let `awaitTurn(ms)` block on the SSE stream instead of re-reading. */
  openStream?: OpenActionsStream;
  gameId: string;
  playerId: string;
  token: string;
  /** Mutated in place; snapshot `{ cursor }` to simulate a restart. */
  state?: BotState;
  /** Optional observer hook after each successful command; used to pace the live demo. */
  onCommandCommitted?: (action: Record<string, unknown>) => void | Promise<void>;
  /**
   * Called whenever durable state changes — before a command is posted, and
   * again once it settles. A runner that only persists between turns would lose
   * the in-flight record in exactly the window it exists to cover.
   */
  onStateChanged?: (state: BotState) => void | Promise<void>;
}

export interface Bot {
  readonly state: BotState;
  /**
   * Take the next batch from the action stream and advance the cursor; return
   * the latest wake seen. With `waitMs` and an `openStream`, block on the SSE
   * stream for up to that long; otherwise read one immediate page.
   */
  awaitTurn(waitMs?: number): Promise<any>;
  /** Take one action if this bot has a legal one right now. */
  step(): Promise<Record<string, unknown> | null>;
  /** Play until this bot has nothing legal left (turn passed, or waiting). */
  playTurn(maxSteps?: number): Promise<void>;
  /** Resolve a pending defence if one is waiting on this bot. */
  defend(): Promise<boolean>;
}

/** Collision-resistant 53-bit string hash → base36 (deterministic, sync). */
function fingerprint(input: string): string {
  let h1 = 0xdeadbeef ^ input.length;
  let h2 = 0x41c6ce57 ^ input.length;
  for (let i = 0; i < input.length; i += 1) {
    const c = input.charCodeAt(i);
    h1 = Math.imul(h1 ^ c, 2654435761);
    h2 = Math.imul(h2 ^ c, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36);
}

interface TerritoryView {
  id: string;
  ownerId?: string;
  armies: number;
  adjacentTerritoryIds?: string[];
}

/**
 * Static geometry, read once from `GET /board`.
 *
 * `/decision` deliberately carries only what changes — ownership, armies, legal
 * actions — so the bot fetches the map from the board surface a single time and
 * caches it. It is immutable after `GameStarted`, so there is nothing to refresh.
 */
type MapView = StrategyMap;

interface Decision {
  mode?: string;
  turn: {
    id: string;
    activePlayerId?: string;
    phase: string;
    reinforcement?: { remaining: number };
  };
  pendingInteraction?: { type: string; attackId: string };
  board: {
    map?: { mapVersion?: string; boardStreamId?: string };
    territories: TerritoryView[];
    players: Array<{ id: string; remainingArmies?: number; eliminated: boolean }>;
  };
  legalMoves: any[];
}

function boardFingerprint(playerId: string, decision: Decision): string {
  const terr = decision.board.territories
    .map((t) => `${t.id}:${t.ownerId ?? "-"}:${t.armies}`)
    .join(",");
  const remaining =
    decision.turn.reinforcement?.remaining ??
    decision.board.players.find((p) => p.id === playerId)?.remainingArmies ??
    0;
  return fingerprint(`${decision.turn.phase}|${remaining}|${terr}`);
}

async function chooseAction(
  playerId: string,
  decision: Decision,
  loadMap: () => Promise<MapView | null>,
): Promise<Record<string, unknown> | null> {
  // Defence first: it is the only out-of-turn action, the deadline is ticking,
  // and it needs no map at all.
  const defense = decision.legalMoves.find((a) => a.type === "roll-defense");
  if (defense) return { type: "roll-defense", attackId: defense.attackId };

  const map = await loadMap();
  if (!map) return null;
  const ctx = strategyContext(playerId, decision.board.territories, map);

  const occupy = decision.legalMoves.find((a) => a.type === "occupy-territory");
  if (occupy) return chooseOccupy(ctx, occupy);

  const reinforce = decision.legalMoves.find((a) => a.type === "reinforce");
  if (reinforce) return chooseReinforce(ctx, reinforce);

  const attack = decision.legalMoves.find((a) => a.type === "declare-attack");
  if (attack) {
    const chosen = chooseAttack(ctx, attack);
    if (chosen) return chosen;
  }

  // No favourable attack anywhere: move idle armies toward one rather than
  // passing the turn, which is what a turtling opponent relies on.
  const fortify = decision.legalMoves.find((a) => a.type === "fortify");
  if (fortify) {
    const chosen = chooseFortify(ctx, fortify);
    if (chosen) return chosen;
  }

  if (decision.legalMoves.some((a) => a.type === "skip-fortifications")) {
    return { type: "skip-fortifications" };
  }
  return null;
}

export function createBot(options: CreateBotOptions): Bot {
  const { call, gameId, playerId, token } = options;
  const state: BotState = options.state ?? {};

  let pendingMessage: ActionRequired | null = null;
  /**
   * Where *this process* has read to. The durable `state.cursor` trails it until
   * the message in hand is answered, so a snapshot taken at any instant resumes
   * at a point whose action is still outstanding rather than past it.
   */
  let liveCursor = state.cursor;

  async function commitCursor(): Promise<void> {
    state.cursor = liveCursor;
    delete state.inflight;
    await options.onStateChanged?.(state);
  }

  /**
   * Re-serializing the stored body reproduces it byte for byte: it was produced
   * by `JSON.stringify`, and a parse/stringify round trip preserves both key
   * order and number formatting for such a value.
   */
  async function postCommand(body: string): Promise<{ status: number; body: any }> {
    return call("POST", `/v1/games/${gameId}/commands`, { token, body: JSON.parse(body) });
  }

  /**
   * Replay a command a previous process posted but never saw acked. The server
   * dedupes on `commandId`, so a commit that did land answers `duplicate` and a
   * lost one is applied now. Only then may the cursor move past its message.
   */
  async function resumeInflight(): Promise<void> {
    const inflight = state.inflight;
    if (!inflight) return;
    const res = await postCommand(inflight.body);
    // 200 is accepted|duplicate. A 4xx means the canonical board already moved
    // past this command — a resolved attack, a passed turn. Either way the ask
    // that produced it is answered and the cursor may advance.
    if (res.status === 200 || (res.status >= 400 && res.status < 500)) {
      liveCursor = inflight.cursorAfter;
      await commitCursor();
    }
  }

  function actionsPath(): string {
    const query = new URLSearchParams();
    if (liveCursor) query.set("offset", liveCursor);
    const suffix = query.toString();
    return `/v1/games/${gameId}/players/me/actions${suffix ? `?${suffix}` : ""}`;
  }

  /** One immediate page — the explicitly negotiated non-streaming reading. */
  async function readPage(): Promise<{ messages: AgentMessage[]; nextOffset?: string } | null> {
    const res = await call("GET", actionsPath(), { token, accept: "application/json" });
    if (res.status !== 200) return null;
    return { messages: res.body.messages ?? [], nextOffset: res.body.nextOffset };
  }

  /**
   * Block on the SSE stream until something arrives or `waitMs` elapses. The
   * first batch carries the backlog, so this is also a catch-up read: a bot that
   * blocks never needs a separate one.
   *
   * `waitMs` is a guard against a server that never closes, not a substitute for
   * the server's own bound — the timer starts here, before the request is even
   * issued, so it is already running through connect, auth and catch-up. A
   * caller passing the server's 30s exactly would abort a hair early on every
   * idle connection and throw away the closing control frame; pass
   * `ACTIONS_STREAM_CLIENT_TIMEOUT_MS`, which carries the transport slack.
   */
  async function readStream(
    waitMs: number,
  ): Promise<{ messages: AgentMessage[]; nextOffset?: string } | null> {
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), waitMs);
    try {
      const response = await options.openStream!(actionsPath(), { token, signal: abort.signal });
      if (response.status !== 200) return null;
      const collected: AgentMessage[] = [];
      let nextOffset: string | undefined;
      for await (const batch of readActionsBatches<AgentMessage>(response)) {
        nextOffset = batch.nextOffset;
        collected.push(...batch.messages);
        // The opening batch is the backlog and may legitimately be empty; keep
        // holding the connection until it produces something or the bound ends.
        if (collected.length > 0 || batch.closed) break;
      }
      return { messages: collected, nextOffset };
    } catch {
      // An aborted wait is an ordinary empty result: nothing happened in time.
      return { messages: [] };
    } finally {
      clearTimeout(timer);
      abort.abort();
    }
  }

  async function awaitTurn(waitMs = 0): Promise<AgentMessage | null> {
    await resumeInflight();
    const page = waitMs > 0 && options.openStream ? await readStream(waitMs) : await readPage();
    if (!page) return null;
    if (page.nextOffset !== undefined) liveCursor = page.nextOffset;
    const messages = page.messages;
    const newest = messages.length > 0 ? messages[messages.length - 1]! : null;
    // An empty page never clears an outstanding ask: the message is still owed
    // a command, and forgetting it here would strand it behind the cursor.
    if (newest) pendingMessage = newest.type === "ActionRequired" ? newest : null;
    if (!pendingMessage) await commitCursor();
    return newest;
  }

  /** Fetch the static map from the board surface once and keep it. */
  let cachedMap: MapView | null = null;
  async function loadMap(): Promise<MapView | null> {
    if (cachedMap) return cachedMap;
    const res = await call("GET", `/v1/games/${gameId}/map`);
    if (res.status !== 200) return null;
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The bot consumes the demo's own authenticated API and rejects non-success responses before narrowing the documented response contract.
    const territories = (res.body.territories ?? []) as Array<{
      id: string;
      continentId: string;
      neighbours: string[];
    }>;
    // Before `GameStarted` there is no map yet; do not cache an empty one.
    if (territories.length === 0) return null;
    cachedMap = {
      territories: territories.map((t) => ({
        id: t.id,
        continentId: t.continentId,
        adjacentTerritoryIds: t.neighbours,
      })),
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The bot consumes the demo's own authenticated API and rejects non-success responses before narrowing the documented response contract.
      continents: (res.body.continents ?? []) as MapView["continents"],
    };
    return cachedMap;
  }

  /**
   * A defence roll is idempotent by construction: the id names the attack, not
   * the board. A duplicate wake, a retry, and a lost race with the canonical
   * timeout therefore all end in the same place — one recorded roll.
   */
  function commandIdFor(action: Record<string, unknown>, decision: Decision): string {
    if (action.type === "roll-defense") return `bot-defense:${String(action.attackId)}`;
    return `${playerId}:${decision.turn.id}:${boardFingerprint(playerId, decision)}`;
  }

  async function step(): Promise<Record<string, unknown> | null> {
    await resumeInflight();
    if (!pendingMessage) await awaitTurn();
    const decision = pendingMessage
      ? // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The bot consumes the demo's own authenticated API and rejects non-success responses before narrowing the documented response contract.
        ({
          mode: pendingMessage.mode,
          turn: pendingMessage.turn,
          pendingInteraction: pendingMessage.pendingInteraction ?? undefined,
          board: pendingMessage.board,
          legalMoves: pendingMessage.legalMoves,
        } as Decision)
      : null;
    if (!decision || decision.legalMoves.length === 0) return null;

    const action = await chooseAction(playerId, decision, loadMap);
    if (!action) return null;

    const body = JSON.stringify({
      commandId: commandIdFor(action, decision),
      turnId: decision.turn.id,
      action,
    });
    // Recorded *before* the POST: a crash between request and response must not
    // lose the fact that this exact command may already be committed.
    state.inflight = { body, cursorAfter: liveCursor };
    await options.onStateChanged?.(state);
    const submitted = await postCommand(body);
    if (submitted.status >= 500) {
      // The server may or may not have committed this. Keep the in-flight record
      // and the cursor exactly where they are, so a retry — in this process or
      // the next one — replays the same bytes and collapses to `duplicate`.
      return null;
    }
    pendingMessage = null;
    await commitCursor();
    if (submitted.status !== 200) {
      // A 4xx means the canonical board moved on — a resolved attack, a passed
      // turn, a closed deadline. All of these are ordinary outcomes for a client
      // acting on a wake, not errors to retry blindly.
      return null;
    }
    await options.onCommandCommitted?.(action);
    return action;
  }

  async function defend(): Promise<boolean> {
    // A defender learns it must roll from its own action stream — the same
    // `defense-required` message an external agent receives.
    if (!pendingMessage) await awaitTurn();
    if (!pendingMessage?.legalMoves.some((a) => a.type === "roll-defense")) return false;
    return (await step()) !== null;
  }

  async function playTurn(maxSteps = 300): Promise<void> {
    for (let taken = 0; taken < maxSteps; taken += 1) {
      const action = await step();
      if (
        !action ||
        action.type === "end-turn" ||
        action.type === "fortify" ||
        action.type === "skip-fortifications"
      ) {
        return;
      }
    }
    throw new Error(`bot ${playerId} exceeded ${maxSteps} steps in one turn`);
  }

  return { state, awaitTurn, step, playTurn, defend };
}
