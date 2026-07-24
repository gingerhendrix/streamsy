/**
 * The signature Risk demo, as a deterministic, reusable scenario (Batch 5).
 *
 * One coherent run over real Streamsy storage + the HTTP command/turn resources
 * that emits structured trace events for the accompanying article and returns a
 * machine-readable summary. It demonstrates, in order:
 *
 *   1. a durable game with two HTTP-only agent players + per-player turn streams;
 *   2. persisting/reloading an agent's notification cursor and continuing;
 *   3. an accepted attack retained with its recorded dice + canonical ack;
 *   4. causal `syncedThrough(ack)` — the board catching up to an acked offset;
 *   5. an idempotent command retry (same commandId → duplicate);
 *   6. a stale/racing command safely rejected;
 *   7. a crash injected immediately after a projection output commit, then
 *      recovery proving the transition was not double-applied;
 *   8. a fresh-generation rebuild verified + cut over, old generation retained;
 *   9. the agent-only game finishing with a winner + final watermark.
 *
 * Agent play uses ONLY the published HTTP resources; the crash + rebuild steps
 * drive the same real canonical stream through the replay-safe projection runtime.
 */

import type { Rng } from "../../src/domain/rng.ts";
import type { StreamProtocolFactory } from "@streamsy/core";
import { compareOffsets } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";

import type { GameEvent } from "../../src/domain/events.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import {
  aggregateBoardView,
  boardsEqual,
  projectionBoardView,
} from "../../src/board/projection.ts";
import { createBoardProjectionAdapter } from "../../src/board/board-projection.ts";
import { createSeededRng } from "../../src/domain/rng.ts";
import { buildApp } from "../http/app.ts";
import { createAgent, type Agent, type HttpCall } from "./agent.ts";
import { readCanonical } from "../game/command-service.ts";
import { rebuildBoardGeneration } from "../game/rebuild.ts";
import { syncedThrough } from "../game/board-sync.ts";
import { boardStreamId, eventStreamId } from "../game/names.ts";
import type { Stores } from "../persistence/stores.ts";

const BASE = "http://risk.demo";

export interface TraceEvent {
  seq: number;
  ts: number;
  step: string;
  [key: string]: unknown;
}

export interface SignatureDemoDeps {
  protocol: StreamProtocolFactory;
  stores: Stores;
  rng?: Rng;
  seed?: number;
  /** Injectable monotonic clock (also stamps trace `ts`). */
  now?: () => number;
  /** Streamed trace sink (e.g. print JSONL). Also collected into the result. */
  emit?: (event: TraceEvent) => void;
}

export interface DemoSummary {
  gameId: string;
  players: Array<{ id: string; name: string }>;
  winnerId: string | null;
  finalWatermark: string | null;
  turnsPlayed: number;
  attackRecorded: {
    from: string;
    to: string;
    attackerRolls: number[];
    defenderRolls: number[];
    territoryCaptured: boolean;
    sourceOffset: string;
  } | null;
  cursorRestart: { playerId: string; cursor: string | undefined; resumed: boolean } | null;
  causalWait: { ackOffset: string; resolvedWatermark: string | null; synced: boolean } | null;
  idempotentRetry: { commandId: string; duplicate: boolean; sameOffset: boolean } | null;
  staleCommand: { code: string; rejected: boolean } | null;
  crashRecovery: {
    crashAtSeq: number;
    /** Canonical events the projection had to apply. */
    canonicalEvents: number;
    /** Change messages committed at the moment of the crash (narrative only). */
    committedAtCrash: number;
    /** Transitions/messages a clean control build of the SAME log produces. */
    expectedTransitions: number;
    expectedOutputMessages: number;
    /** What the crashed-then-recovered generation actually contains. */
    actualTransitions: number;
    actualOutputMessages: number;
    /** Source ordinals applied more than once — must be empty. */
    duplicateSourceSeqs: number[];
    /** True if any transition was applied twice (see fields above). */
    doubleApplied: boolean;
    boardEqual: boolean;
    watermarkEqual: boolean;
  };
  rebuild: {
    fromGeneration: string;
    toGeneration: string;
    activeGeneration: string;
    retained: string[];
    boardEqual: boolean;
    watermarkEqual: boolean;
  };
}

export interface SignatureDemoResult {
  summary: DemoSummary;
  trace: TraceEvent[];
}

interface Capture {
  commandId: string;
  turnId: string;
  playerId: string;
  token: string;
  sourceOffset: string;
  events: GameEvent[];
  request: unknown;
}

/**
 * What a projection stream actually contains, decoded from its committed bytes.
 *
 * Every transition appends exactly one `projectionMeta` row carrying the applied
 * `sourceSeq`, so counting those rows — and looking for a repeated `sourceSeq` —
 * is a *direct* test for a double-applied transition, independent of how many
 * change messages a transition happens to produce.
 */
export interface ProjectionOutputAnalysis {
  /** Total Durable-State change messages committed to the stream. */
  outputMessages: number;
  /** One per applied transition (`projectionMeta` rows). */
  transitions: number;
  /** The applied source ordinals, in stream order. */
  sourceSeqs: number[];
  /** Any source ordinal applied more than once — must be empty. */
  duplicateSourceSeqs: number[];
  /** The watermark of the last transition. */
  lastSourceThroughOffset: string | null;
}

type ProjectionRow = {
  type?: string;
  value?: { sourceSeq?: number; sourceThroughOffset?: string };
};
const projectionRowSchema: JsonCodec<ProjectionRow> = {
  encode: (value) => value,
  decode: (value) => value as ProjectionRow,
};

/**
 * True when `actual` shows a transition applied more than once, judged against a
 * clean `control` build of the same canonical log. A double-apply repeats a
 * `sourceSeq` and/or pushes the transition/message counts above the control — so
 * unlike a bare "did the count grow?" check, this discriminates.
 */
export function detectDoubleApply(
  actual: ProjectionOutputAnalysis,
  control: ProjectionOutputAnalysis,
): boolean {
  return (
    actual.duplicateSourceSeqs.length > 0 ||
    actual.transitions !== control.transitions ||
    actual.outputMessages !== control.outputMessages
  );
}

/** Decode a projection stream into {@link ProjectionOutputAnalysis}. */
export async function analyzeProjectionOutput(
  protocol: StreamProtocolFactory,
  streamId: string,
): Promise<ProjectionOutputAnalysis> {
  const empty: ProjectionOutputAnalysis = {
    outputMessages: 0,
    transitions: 0,
    sourceSeqs: [],
    duplicateSourceSeqs: [],
    lastSourceThroughOffset: null,
  };
  const got = await createJsonProtocol(protocol, projectionRowSchema).get(streamId);
  if (got.status !== "ok") return empty;
  const history = await got.stream.readAll();
  const sourceSeqs: number[] = [];
  let lastSourceThroughOffset: string | null = null;
  for (const row of history.values) {
    if (row.type === "projectionMeta" && typeof row.value?.sourceSeq === "number") {
      sourceSeqs.push(row.value.sourceSeq);
      lastSourceThroughOffset = row.value.sourceThroughOffset ?? null;
    }
  }

  const seen = new Set<number>();
  const duplicateSourceSeqs: number[] = [];
  for (const s of sourceSeqs) {
    if (seen.has(s)) duplicateSourceSeqs.push(s);
    else seen.add(s);
  }

  return {
    outputMessages: history.messages.length,
    transitions: sourceSeqs.length,
    sourceSeqs,
    duplicateSourceSeqs,
    lastSourceThroughOffset,
  };
}

export async function runSignatureDemo(deps: SignatureDemoDeps): Promise<SignatureDemoResult> {
  const rng: Rng = deps.rng ?? createSeededRng(deps.seed ?? 1234);
  let tick = 0;
  const now = deps.now ?? (() => (tick += 1));
  const app = buildApp({ protocol: deps.protocol, stores: deps.stores, rng, now });

  const trace: TraceEvent[] = [];
  let seq = 0;
  const emit = (step: string, data: Record<string, unknown>): void => {
    const event: TraceEvent = { seq: (seq += 1), ts: now(), step, ...data };
    trace.push(event);
    deps.emit?.(event);
  };

  // Capturing HTTP client: records every accepted command ack transparently.
  const captures: Capture[] = [];
  const playerByToken = new Map<string, string>();
  const call: HttpCall = async (method, path, opts = {}) => {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (opts.token) headers.authorization = `Bearer ${opts.token}`;
    const init: RequestInit = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
    const res = await app.fetch(new Request(`${BASE}${path}`, init));
    const body = await res.json().catch(() => ({}));
    if (
      method === "POST" &&
      path.endsWith("/commands") &&
      res.status === 200 &&
      body.status === "accepted"
    ) {
      captures.push({
        commandId: body.commandId,
        turnId: body.turnId,
        playerId: playerByToken.get(opts.token ?? "") ?? "",
        token: opts.token ?? "",
        sourceOffset: body.sourceOffset,
        events: body.events ?? [],
        request: opts.body,
      });
    }
    return { status: res.status, body };
  };

  // --- 1. durable game with two HTTP-only agents ---------------------------
  const created = await call("POST", "/v1/games", {
    body: { ruleset: "risk-demo-v1", name: "Ada", color: "red" },
  });
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };
  const joined = await call("POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  const guestId: string = joined.body.player.id;
  tokenByPlayer[guestId] = joined.body.capability;
  const players = [hostId, guestId];
  for (const id of players) playerByToken.set(tokenByPlayer[id]!, id);
  await call("POST", `/v1/games/${gameId}/start`, { token: tokenByPlayer[hostId], body: {} });
  emit("game-started", {
    gameId,
    players: players.map((id) => ({ id, name: id === hostId ? "Ada" : "Bob" })),
  });

  // --- 2. initial turn notifications for each player -----------------------
  for (const id of players) {
    const turns = await call("GET", `/v1/games/${gameId}/players/me/turns`, {
      token: tokenByPlayer[id],
    });
    emit("turn-notifications", {
      playerId: id,
      wakes: turns.body.notifications.length,
      firstTurnId: turns.body.notifications[0]?.turnId ?? null,
    });
  }

  const agents: Record<string, Agent> = {};
  for (const id of players) {
    agents[id] = createAgent({ call, gameId, playerId: id, token: tokenByPlayer[id]!, state: {} });
  }

  // Interleaved-demo state.
  let attackRecorded: DemoSummary["attackRecorded"] = null;
  let cursorRestart: DemoSummary["cursorRestart"] = null;
  let causalWait: DemoSummary["causalWait"] = null;
  let staleCommand: DemoSummary["staleCommand"] = null;
  let staleSeed: { turnId: string; playerId: string; token: string; round: number } | null = null;
  let turnsPlayed = 0;

  let finished = false;
  for (let guard = 0; guard < 800 && !finished; guard += 1) {
    const meta = await call("GET", `/v1/games/${gameId}`);
    if (meta.body.status === "finished") {
      finished = true;
      break;
    }
    const active: string = meta.body.activePlayerId;

    // 6: a stale command — the same player, now on a later turn, replays its own
    // past-round `turnId` (as a delayed/duplicate wake would). Canonical turn
    // validation rejects it `STALE_TURN`; no invalid state can commit.
    if (
      staleSeed &&
      !staleCommand &&
      active === staleSeed.playerId &&
      meta.body.round > staleSeed.round
    ) {
      const rej = await call("POST", `/v1/games/${gameId}/commands`, {
        token: staleSeed.token,
        body: {
          commandId: `stale-${staleSeed.turnId}`,
          turnId: staleSeed.turnId,
          action: { type: "end-turn" },
        },
      });
      staleCommand = { code: rej.body.error?.code ?? "UNKNOWN", rejected: rej.status !== 200 };
      emit("stale-command-rejected", {
        playerId: staleSeed.playerId,
        staleTurnId: staleSeed.turnId,
        currentRound: meta.body.round,
        httpStatus: rej.status,
        code: staleCommand.code,
      });
    }

    // 2: persist + reload one agent's notification cursor, then continue.
    if (!cursorRestart && active === players[0] && guard >= 2) {
      const savedCursor = agents[active]!.state.cursor;
      agents[active] = createAgent({
        call,
        gameId,
        playerId: active,
        token: tokenByPlayer[active]!,
        state: { cursor: savedCursor }, // rebuilt from ONLY the persisted cursor
      });
      cursorRestart = { playerId: active, cursor: savedCursor, resumed: true };
      emit("agent-cursor-restart", { playerId: active, cursor: savedCursor ?? null });
    }

    const agent = agents[active]!;
    const wake = await agent.awaitTurn();
    emit("turn-wake", { playerId: active, round: meta.body.round, turnId: wake?.turnId ?? null });

    const acksBefore = captures.length;
    await agent.playTurn();
    turnsPlayed += 1;
    const fresh = captures.slice(acksBefore);

    // Seed the stale-command demo from the first accepted turn.
    if (!staleSeed && fresh[0]) {
      staleSeed = {
        turnId: fresh[0].turnId,
        playerId: active,
        token: tokenByPlayer[active]!,
        round: meta.body.round,
      };
    }

    // 3: retain the first accepted attack with its recorded dice + ack.
    if (!attackRecorded) {
      for (const c of fresh) {
        const attack = c.events.find((e) => e.type === "AttackResolved");
        if (attack && attack.type === "AttackResolved") {
          attackRecorded = {
            from: attack.from,
            to: attack.to,
            attackerRolls: attack.attackerRolls,
            defenderRolls: attack.defenderRolls,
            territoryCaptured: attack.territoryCaptured,
            sourceOffset: c.sourceOffset,
          };
          emit("attack-recorded", { ...attackRecorded, commandId: c.commandId });
          break;
        }
      }
    }

    // 4: causal wait — the board projection catches up to an acked offset.
    if (!causalWait && fresh[0]) {
      const ack = { sourceStreamId: eventStreamId(gameId), sourceOffset: fresh[0].sourceOffset };
      const board = await syncedThrough(call, gameId, ack, { timeoutMs: 2_000 });
      const synced =
        board.sourceThroughOffset !== null &&
        compareOffsets(board.sourceThroughOffset, ack.sourceOffset) >= 0;
      causalWait = {
        ackOffset: ack.sourceOffset,
        resolvedWatermark: board.sourceThroughOffset,
        synced,
      };
      emit("causal-wait-resolved", causalWait);
    }
  }

  const finalMeta = await call("GET", `/v1/games/${gameId}`);
  const finalBoard = await call("GET", `/v1/games/${gameId}/board`);
  emit("game-finished", {
    winnerId: finalMeta.body.winnerId ?? null,
    finalWatermark: finalBoard.body.sourceThroughOffset ?? null,
    generation: finalBoard.body.generation,
    turnsPlayed,
  });

  // --- 5. idempotent command retry (same commandId → duplicate) ------------
  let idempotentRetry: DemoSummary["idempotentRetry"] = null;
  const firstAck = captures[0];
  if (firstAck) {
    const retry = await call("POST", `/v1/games/${gameId}/commands`, {
      token: firstAck.token,
      body: firstAck.request,
    });
    idempotentRetry = {
      commandId: firstAck.commandId,
      duplicate: retry.body.status === "duplicate",
      sameOffset: retry.body.sourceOffset === firstAck.sourceOffset,
    };
    emit("idempotent-retry", idempotentRetry);
  }

  // --- 7. crash immediately after a projection output commit ---------------
  const { events } = await readCanonical(deps.protocol, eventStreamId(gameId));
  const crashGen = "crash-demo";
  const crashStreamId = boardStreamId(gameId, crashGen);
  const crashAtSeq = Math.max(1, Math.floor(events.length / 2));
  const crashing = new ProjectionRuntime({
    protocol: deps.protocol,
    adapter: createBoardProjectionAdapter({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId: crashStreamId,
      generation: crashGen,
    }),
    faults: {
      afterAppend: ({ sourceSeq }) => {
        if (sourceSeq === crashAtSeq)
          throw new Error("injected crash immediately after output commit");
      },
    },
  });
  let crashed = false;
  try {
    await crashing.catchUp();
  } catch {
    crashed = true;
  }
  const atCrash = await analyzeProjectionOutput(deps.protocol, crashStreamId);
  const recovered = new ProjectionRuntime({
    protocol: deps.protocol,
    adapter: createBoardProjectionAdapter({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId: crashStreamId,
      generation: crashGen,
    }),
  });
  await recovered.catchUp();
  const afterRecovery = await analyzeProjectionOutput(deps.protocol, crashStreamId);

  // Control: materialize the SAME canonical log into a clean generation that
  // never crashed. It defines exactly what the crashed generation should contain.
  const controlGen = "crash-control";
  const controlStreamId = boardStreamId(gameId, controlGen);
  const control = new ProjectionRuntime({
    protocol: deps.protocol,
    adapter: createBoardProjectionAdapter({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId: controlStreamId,
      generation: controlGen,
    }),
  });
  await control.catchUp();
  const controlAnalysis = await analyzeProjectionOutput(deps.protocol, controlStreamId);

  const recoveredView = projectionBoardView(recovered.currentState());
  const authoritativeView = aggregateBoardView(foldAggregate(events));
  const doubleApplied = detectDoubleApply(afterRecovery, controlAnalysis);
  const crashRecovery = {
    crashAtSeq,
    canonicalEvents: events.length,
    committedAtCrash: atCrash.outputMessages,
    expectedTransitions: controlAnalysis.transitions,
    expectedOutputMessages: controlAnalysis.outputMessages,
    actualTransitions: afterRecovery.transitions,
    actualOutputMessages: afterRecovery.outputMessages,
    duplicateSourceSeqs: afterRecovery.duplicateSourceSeqs,
    doubleApplied,
    boardEqual: boardsEqual(recoveredView, authoritativeView),
    watermarkEqual:
      afterRecovery.lastSourceThroughOffset === controlAnalysis.lastSourceThroughOffset,
  };
  emit("crash-recovery", { ...crashRecovery, crashed, controlGeneration: controlGen });

  // --- 8. rebuild into a fresh generation, verify, cut over ----------------
  const rebuilt = await rebuildBoardGeneration(
    { protocol: deps.protocol, stores: deps.stores },
    gameId,
    {
      now,
    },
  );
  const rebuild = {
    fromGeneration: rebuilt.fromGeneration,
    toGeneration: rebuilt.toGeneration,
    activeGeneration: rebuilt.activeGeneration,
    retained: rebuilt.retainedGenerations,
    boardEqual: rebuilt.equivalence.boardEqual,
    watermarkEqual: rebuilt.equivalence.watermarkEqual,
  };
  emit("generation-rebuild", { status: rebuilt.status, ...rebuild });

  // Reads use the newly-active generation but the same logical board.
  const rebuiltBoard = await call("GET", `/v1/games/${gameId}/board`);
  emit("post-cutover-board", {
    generation: rebuiltBoard.body.generation,
    sourceThroughOffset: rebuiltBoard.body.sourceThroughOffset ?? null,
  });

  const summary: DemoSummary = {
    gameId,
    players: players.map((id) => ({ id, name: id === hostId ? "Ada" : "Bob" })),
    winnerId: finalMeta.body.winnerId ?? null,
    finalWatermark: finalBoard.body.sourceThroughOffset ?? null,
    turnsPlayed,
    attackRecorded,
    cursorRestart,
    causalWait,
    idempotentRetry,
    staleCommand,
    crashRecovery,
    rebuild,
  };
  emit("summary", { ...summary });
  return { summary, trace };
}
