/**
 * The `Hex Domination` playing surface: hex map, phase interactions, current-turn
 * column, and the defence/dice experience.
 *
 * The screen is three regions under one thin match bar, and each answers exactly one
 * question:
 *
 *   match bar   round, seat, phase — the frame for everything else
 *   left        *what am I being asked to do?* — one section per phase, only the
 *               active one carrying instructions and controls
 *   middle      *where?* — the map, its focused-country detail, and command notices
 *   right       *how does it stand?* — army/continent standings and history
 *
 * Interaction follows one model everywhere — **source → target → amount →
 * confirm** — rather than generating a button per legal move. Legality comes from
 * the player-relative decision resource, so this screen never decides what is legal;
 * it decides what is *clickable*, and the canonical command validation has the
 * final word either way.
 *
 * Two current-specific shapes drive most of the state here:
 *
 *  - **Defence is out-of-turn.** The decision resource is player-relative, so this
 *    screen refetches it on every board change rather than only when the seat is
 *    the active player — a defender is asked to act during someone else's turn.
 *  - **Combat clears canonically.** The projection drops the `combat` row the moment
 *    an attack closes, so the reveal is held client-side and keyed to the throw's
 *    own identity: a result that had already happened when this screen opened is
 *    shown, not re-animated.
 */

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import type {
  AgentSeatResponse,
  BoardResponse,
  CommandAck,
  DecisionResponse,
  GameResponse,
  JoinGameResponse,
  PlayAction,
  PlayCommandRequest,
} from "../application/api.ts";
import type { LegalAction } from "../application/legal-actions.ts";
import type { ProjectedHex } from "../board/projection.ts";
import { ackTxId } from "../board/transaction.ts";
import { RULES } from "../domain/map.ts";
import {
  attackAgainAction,
  attackTerritoryIds,
  fortifyAction as canonicalFortifyAction,
  shouldDismissAttackSummary,
} from "./attack-phase.ts";
import {
  latestAttackTrace,
  openedThroughWatermark,
  traceToDraw,
  type WatermarkLatch,
  type WatermarkRead,
} from "./attack-trace.ts";
import { useRiskBoardStream } from "./board-stream-db.ts";
import { CombatCard } from "./combat-card.tsx";
import { combatView } from "./combat-view.ts";
import {
  HexMap,
  countryLabel,
  territoryInteractionState,
  type MapTerritory,
  type TerritoryInteractionState,
} from "./hex-map.tsx";
import { MapControls } from "./map-controls.tsx";
import { MatchBar } from "./match-bar.tsx";
import {
  ReinforcementPlacement,
  adjustPendingReinforcements,
  pendingReinforcementTotal,
  type PendingReinforcements,
} from "./reinforcement-placement.tsx";
import {
  revealPlan,
  seatStatusLabel,
  terrainMix,
  type NameLookup,
  type RevealPlan,
} from "./presentation.ts";
import { Lobby, type AgentSeat } from "./lobby.tsx";
import {
  SyncPill,
  TopBar,
  acknowledgementNotice,
  agentSeatName,
  api,
  errorMessage,
  isError,
  spectatingSeat,
  type Identity,
} from "./shared.tsx";
import { StatusColumn } from "./status-column.tsx";
import { TurnColumn, VictoryCard } from "./turn-column.tsx";

type Selection =
  | null
  | { kind: "attack"; from: string; to?: string; dice: number }
  | { kind: "fortify"; from: string; to?: string; armies: number };

/** Which manoeuvre a click means during the attack phase; both share sources. */
type Intent = "attack" | "fortify";

export type { AgentSeat } from "./lobby.tsx";

export interface GameScreenProps {
  gameId: string;
  game: GameResponse;
  identity: Identity | null;
  onIdentity(next: Identity | null): void;
  refreshGame(): Promise<GameResponse | null>;
  name: string;
  onName(value: string): void;
  onCopyInvite(): Promise<void>;
  initialAgentSeats?: AgentSeat[];
}

function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(
    () => window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true,
  );
  useEffect(() => {
    const query = window.matchMedia?.("(prefers-reduced-motion: reduce)");
    if (!query) return;
    const onChange = (): void => setReduced(query.matches);
    query.addEventListener("change", onChange);
    return () => query.removeEventListener("change", onChange);
  }, []);
  return reduced;
}

/**
 * The source-stream watermark canonical history stood at when this screen opened.
 *
 * Read once from `GET /board`, which is served without cache headers, so the answer
 * is canonical rather than whatever a cache still holds — the whole point, since the
 * projection stream's own first response *is* cacheable and is what made a historic
 * throw look new. The response is otherwise unused: only its watermark is kept.
 *
 * Three values, and the distinction matters at both ends:
 *   `undefined` — not known yet (or in flight); no animation may claim to be news.
 *   `null`      — canonical history was empty; every throw since is news.
 *   an offset   — everything at or below it is history.
 *
 * `fallbackOffset` keeps the feature alive if that read fails: the projection's own
 * watermark, which is the best the live collections can offer and is exactly as
 * lag-prone as the state carrying it. It is latched at the first render that can
 * offer one and never re-read — `openedThroughWatermark` explains why passing it
 * through live would silently delete the overlay rather than degrade it, which is
 * what an earlier cut of this hook did.
 */
function useOpenedThroughOffset(
  gameId: string,
  fallbackOffset: string | undefined,
): string | null | undefined {
  const [read, setRead] = useState<WatermarkRead>({ state: "reading" });
  // Not state: writing it must not itself schedule a render, and it is written once.
  const latch = useRef<WatermarkLatch>({});
  useEffect(() => {
    setRead({ state: "reading" });
    latch.current = {};
    let cancelled = false;
    void fetch(`/v1/games/${gameId}/board`, { cache: "no-store" })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error("no board"))))
      .then((body: BoardResponse) => {
        if (!cancelled) setRead({ state: "known", offset: body.sourceThroughOffset ?? null });
      })
      .catch(() => {
        if (!cancelled) setRead({ state: "failed" });
      });
    return () => {
      cancelled = true;
    };
  }, [gameId]);
  return openedThroughWatermark(read, fallbackOffset, latch.current);
}

export function detailTerritoryId(
  hoveredId: string | null,
  focusedId: string | null,
): string | null {
  return hoveredId ?? focusedId;
}

function findAction<T extends LegalAction["type"]>(
  actions: LegalAction[] | undefined,
  type: T,
): Extract<LegalAction, { type: T }> | undefined {
  return actions?.find(
    (action): action is Extract<LegalAction, { type: T }> => action.type === type,
  );
}

export function GameScreen(props: GameScreenProps) {
  const { gameId, identity } = props;
  const live = useRiskBoardStream(props.game.boardStreamId ?? null);
  const board = live.rows;

  const [decision, setDecision] = useState<DecisionResponse | null>(null);
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [pendingReinforcements, setPendingReinforcements] = useState<PendingReinforcements>(
    () => new Map(),
  );
  const [selection, setSelection] = useState<Selection>(null);
  const [intent, setIntent] = useState<Intent>("attack");
  const [dismissedCombatId, setDismissedCombatId] = useState<string | null>(null);
  const [occupyArmies, setOccupyArmies] = useState<number | null>(null);
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  // A lobby may open more than one agent seat — an agent-versus-agent game is how
  // the demo runs itself to a winner without a human at the keyboard.
  const [agentSeats, setAgentSeats] = useState<AgentSeat[]>(() => props.initialAgentSeats ?? []);
  useEffect(() => {
    if (!props.initialAgentSeats?.length) return;
    setAgentSeats((current) => {
      const incoming = new Map(props.initialAgentSeats!.map((seat) => [seat.playerId, seat]));
      return [...current.filter((seat) => !incoming.has(seat.playerId)), ...incoming.values()];
    });
  }, [props.initialAgentSeats]);
  const reducedMotion = usePrefersReducedMotion();

  const offset = board?.meta?.sourceThroughOffset ?? null;

  // ---- seat identity --------------------------------------------------------
  // Derived before the decision fetch because a spectator has no decision to make:
  // the delegated host of an agent-versus-agent game still holds a capability the
  // server would happily answer for, and asking would hand this screen legal moves
  // for a seat the agent is playing.
  const selfSeat = identity
    ? (board?.players ?? []).find((player) => player.id === identity.playerId)
    : undefined;
  const spectating = spectatingSeat(identity, selfSeat);
  const selfId = spectating ? undefined : identity?.playerId;

  // ---- decision (player-relative: a defender acts out of turn) --------------
  useEffect(() => {
    if (!identity || spectating) {
      setDecision(null);
      return;
    }
    const controller = new AbortController();
    void api<DecisionResponse>("GET", `/v1/games/${gameId}/decision`, {
      token: identity.token,
    }).then((result) => {
      if (controller.signal.aborted) return;
      if (result.status === 200 && !isError(result.body)) setDecision(result.body);
    });
    return () => controller.abort();
  }, [gameId, identity, spectating, offset, board?.game.status]);

  const legalActions = decision?.legalMoves;
  const reinforceAction = findAction(legalActions, "reinforce");
  const attackAction = findAction(legalActions, "declare-attack");
  const fortifyAction = findAction(legalActions, "fortify");
  const occupyAction = findAction(legalActions, "occupy-territory");
  const defenseAction = findAction(legalActions, "roll-defense");
  const skipFortificationsAction = findAction(legalActions, "skip-fortifications");

  const turnId = board?.turn?.turnId ?? decision?.turn.id ?? "";

  // A new turn is a fresh decision surface; nothing selected carries across.
  useEffect(() => {
    setSelection(null);
    setPendingReinforcements(new Map());
    setIntent("attack");
    setDismissedCombatId(null);
  }, [turnId]);

  const reinforcementAvailable = reinforceAction !== undefined;
  useEffect(() => {
    if (!reinforcementAvailable) setPendingReinforcements(new Map());
  }, [reinforcementAvailable]);

  useEffect(() => {
    setOccupyArmies(occupyAction ? occupyAction.minArmies : null);
  }, [occupyAction?.attackId, occupyAction?.minArmies]);

  // ---- combat + reveal ------------------------------------------------------
  const combat = useMemo(
    () =>
      board
        ? combatView({
            combat: board.combat,
            turn: board.turn,
            moves: board.moves,
          })
        : null,
    [board],
  );
  const visibleCombat = combat?.attackId === dismissedCombatId ? null : combat;

  const seenReveals = useRef(new Set<string>());

  // The map's own record of the newest throw: route plus both losses, read from the
  // move feed.
  const trace = useMemo(() => latestAttackTrace(board?.moves ?? []), [board?.moves]);

  const revealKey = combat ? `${combat.attackId}:${combat.status}` : null;

  // How far canonical history had run when this screen opened. Both the dice reveal
  // and the map trace are animations of something happening *now*, so both are
  // suppressed for anything that had already happened — and "already happened" cannot
  // be read off the stream, whose first response a browser may answer from a
  // `max-age=60` cache with an internally coherent but stale state that claims to be
  // up to date. Every throw committed since arrives afterwards looking like news, which
  // is what redrew a historic throw at first paint. So the watermark is taken from the
  // board resource, which is served uncacheable, and the comparison is offset against
  // offset. `undefined` while it is in flight: nothing is drawn until it is known.
  const openedThrough = useOpenedThroughOffset(gameId, board?.meta?.sourceThroughOffset);
  const historyNamed = openedThrough !== undefined;
  const revealSeeded = useRef(false);
  if (!revealSeeded.current && historyNamed) {
    revealSeeded.current = true;
    if (revealKey) seenReveals.current.add(revealKey);
  }

  const [reveal, setReveal] = useState<RevealPlan>({
    mode: "none",
    durationMs: 0,
  });
  useEffect(() => {
    if (!revealKey) return;
    // Until history has been named, a reveal cannot be told from a replay of one.
    if (!historyNamed) return;
    const alreadySeen = seenReveals.current.has(revealKey);
    seenReveals.current.add(revealKey);
    setReveal(revealPlan({ reducedMotion, alreadySeen }));
  }, [revealKey, reducedMotion, historyNamed]);

  const visibleTrace = traceToDraw(trace, openedThrough);

  // The countdown only needs to tick while a defence window is actually open.
  const pendingDeadline =
    combat?.status === "awaiting-defense" ? combat.defenseDeadlineAt : undefined;
  useEffect(() => {
    if (pendingDeadline === undefined) return;
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), 250);
    return () => window.clearInterval(timer);
  }, [pendingDeadline]);

  // ---- lookups --------------------------------------------------------------
  const playerById = useMemo(
    () => new Map((board?.players ?? []).map((player) => [player.id, player])),
    [board?.players],
  );
  const territoryById = useMemo(
    () => new Map((board?.territories ?? []).map((territory) => [territory.id, territory])),
    [board?.territories],
  );
  const continentById = useMemo(
    () => new Map((board?.continents ?? []).map((continent) => [continent.id, continent])),
    [board?.continents],
  );
  const hexesByTerritory = useMemo(() => {
    const grouped = new Map<string, ProjectedHex[]>();
    for (const hex of board?.hexes ?? []) {
      const bucket = grouped.get(hex.territoryId);
      if (bucket) bucket.push(hex);
      else grouped.set(hex.territoryId, [hex]);
    }
    return grouped;
  }, [board?.hexes]);

  const names: NameLookup = useMemo(
    () => ({
      territory: (id) => territoryById.get(id)?.name ?? id,
      player: (id) => (id ? (playerById.get(id)?.name ?? "A player") : "Nobody"),
      continent: (id) => continentById.get(id)?.name ?? id,
    }),
    [territoryById, playerById, continentById],
  );
  const colorOf = useCallback(
    (playerId: string | undefined) => playerById.get(playerId ?? "")?.color ?? "#5b6a7d",
    [playerById],
  );
  const controllerOf = useCallback(
    (playerId: string | undefined) => playerById.get(playerId ?? "")?.controller,
    [playerById],
  );

  // ---- commands -------------------------------------------------------------
  const submit = useCallback(
    async (action: PlayAction, commandId?: string) => {
      if (!identity || !turnId) return false;
      setBusy(true);
      const body: PlayCommandRequest = {
        commandId: commandId ?? `${identity.playerId}:${turnId}:${crypto.randomUUID().slice(0, 8)}`,
        turnId,
        action,
      };
      const result = await api<CommandAck>("POST", `/v1/games/${gameId}/commands`, {
        token: identity.token,
        body,
      });
      const accepted = result.status === 200 && !isError(result.body);
      if (accepted) {
        try {
          if (!live.session) throw new Error("Board session is not connected.");
          await live.session.awaitTxId(ackTxId(result.body as CommandAck));
          setNotice(acknowledgementNotice(action.type));
        } catch {
          setNotice(`${acknowledgementNotice(action.type)} Live board still catching up.`);
        }
      } else {
        setNotice(errorMessage(result.body, "Move rejected."));
      }
      setBusy(false);
      return accepted;
    },
    [gameId, identity, turnId, live.session],
  );

  const interactionBusy = busy;

  const adjustReinforcement = useCallback(
    (territoryId: string, delta: 1 | -1) => {
      if (!reinforceAction || interactionBusy) return;
      setPendingReinforcements((current) =>
        adjustPendingReinforcements(
          current,
          territoryId,
          delta,
          reinforceAction.territoryIds,
          reinforceAction.pool,
        ),
      );
    },
    [reinforceAction, interactionBusy],
  );

  const finishReinforcements = useCallback(async () => {
    if (
      !reinforceAction ||
      interactionBusy ||
      pendingReinforcementTotal(pendingReinforcements) !== reinforceAction.pool
    ) {
      return;
    }

    const placements = reinforceAction.territoryIds.flatMap((territoryId) => {
      const armies = pendingReinforcements.get(territoryId) ?? 0;
      return armies > 0 ? [{ territoryId, armies }] : [];
    });

    const accepted = await submit({ type: "reinforce", placements });
    if (accepted) setPendingReinforcements(new Map());
  }, [reinforceAction, interactionBusy, pendingReinforcements, submit]);

  const startGame = async () => {
    if (!identity) return;
    setBusy(true);
    const result = await api<CommandAck>("POST", `/v1/games/${gameId}/start`, {
      token: identity.token,
      body: {},
    });
    if (result.status === 200 && !isError(result.body)) {
      try {
        if (!live.session) throw new Error("Board session is not connected.");
        await live.session.awaitTxId(ackTxId(result.body));
        setNotice("Game started — the map is dealt.");
      } catch {
        setNotice("Game started, but the live board is still catching up.");
      }
    } else {
      setNotice(errorMessage(result.body, "Could not start the game."));
    }
    setBusy(false);
    await props.refreshGame();
  };

  const joinGame = async () => {
    setBusy(true);
    // No colour is requested: the server assigns a free palette colour
    // canonically, so simultaneous joins cannot collide on a swatch.
    const result = await api<JoinGameResponse>("POST", `/v1/games/${gameId}/players`, {
      body: { name: props.name },
    });
    setBusy(false);
    if (result.status !== 201 || isError(result.body)) {
      setNotice(errorMessage(result.body, "Could not join the game."));
      return;
    }
    props.onIdentity({
      gameId,
      playerId: result.body.player.id,
      token: result.body.capability,
      role: "player",
    });
    await props.refreshGame();
  };

  /** Open an external-agent seat and hand back its private bootstrap URL. */
  const addAgentSeat = async (requestedName: string) => {
    setBusy(true);
    // The ordinal is the seat number on the muster roll, not the number of seats
    // this browser happens to have opened, so an unnamed agent is called after the
    // row it will occupy.
    const seat = (board?.players.length ?? agentSeats.length) + 1;
    // The agent seat's colour is assigned server-side like every other seat.
    const result = await api<AgentSeatResponse>("POST", `/v1/games/${gameId}/agent-seats`, {
      token: identity?.token,
      body: { name: agentSeatName(requestedName, seat) },
    });
    setBusy(false);
    if (result.status !== 201 || isError(result.body)) {
      setNotice(errorMessage(result.body, "Could not open an agent seat."));
      return;
    }
    const opened: AgentSeat = {
      playerId: result.body.seat.playerId,
      name: result.body.seat.name,
      instructions: result.body.instructions,
    };
    setAgentSeats((seats) => [...seats, opened]);
    setNotice("Agent seat opened — copy its instructions into your coding-agent UI.");
  };

  // ---- map interaction ------------------------------------------------------
  const attackChoicesFrom = useCallback(
    (from: string) => attackAction?.choices.filter((choice) => choice.from === from) ?? [],
    [attackAction],
  );
  const fortifyChoiceFrom = useCallback(
    (from: string) => fortifyAction?.choices.find((choice) => choice.from === from),
    [fortifyAction],
  );
  const repeatAttack = useMemo(
    () => attackAgainAction(attackAction, combat),
    [attackAction, combat],
  );

  const actionable = useMemo(() => {
    const ids = new Set<string>();
    if (occupyAction) return ids;
    if (reinforceAction) {
      for (const id of reinforceAction.territoryIds) ids.add(id);
      return ids;
    }
    if (selection?.kind === "attack") {
      return attackTerritoryIds(attackAction, selection.from);
    } else if (selection?.kind === "fortify") {
      ids.add(selection.from);
      for (const reachable of fortifyChoiceFrom(selection.from)?.reachable ?? []) {
        ids.add(reachable.to);
      }
    } else if (intent === "attack") {
      return attackTerritoryIds(attackAction);
    } else {
      for (const choice of fortifyAction?.choices ?? []) ids.add(choice.from);
    }
    return ids;
  }, [
    occupyAction,
    reinforceAction,
    selection,
    intent,
    attackAction,
    fortifyAction,
    attackChoicesFrom,
    fortifyChoiceFrom,
  ]);

  const activeTerritories = useMemo(() => {
    const ids = new Set<string>();
    for (const [id, count] of pendingReinforcements) if (count > 0) ids.add(id);
    if (selection) {
      ids.add(selection.from);
      if (selection.to) ids.add(selection.to);
    }
    if (occupyAction) {
      ids.add(occupyAction.from);
      ids.add(occupyAction.to);
    }
    if (combat && combat.status !== "resolved") {
      ids.add(combat.from);
      ids.add(combat.to);
    }
    return ids;
  }, [pendingReinforcements, selection, occupyAction, combat]);

  // A decision is visually narrowed only while this local seat is choosing a
  // territory. Waiting/spectating seats and amount-only occupation/defence steps
  // retain the normal map, with any canonical combat participants still active.
  const choosingTerritory =
    decision?.mode === "active-turn" &&
    Boolean(
      reinforceAction ||
      selection ||
      (intent === "attack" ? attackAction?.choices.length : fortifyAction?.choices.length),
    );

  const stateOf = useCallback(
    (id: string): TerritoryInteractionState =>
      territoryInteractionState({
        active: activeTerritories.has(id),
        hovered: hoveredId === id,
        choosing: choosingTerritory,
        actionable: actionable.has(id),
      }),
    [activeTerritories, hoveredId, choosingTerritory, actionable],
  );

  const onSelect = useCallback(
    (id: string) => {
      setFocusedId(id);
      if (occupyAction || combat?.status === "awaiting-defense") return;

      if (reinforceAction?.territoryIds.includes(id)) {
        adjustReinforcement(id, 1);
        return;
      }

      if (selection?.kind === "attack") {
        if (id === selection.from) return setSelection(null);
        const choice = attackChoicesFrom(selection.from).find((option) => option.to === id);
        if (choice) {
          return setSelection({
            ...selection,
            to: id,
            dice: choice.maxAttackerDice,
          });
        }
      }
      if (selection?.kind === "fortify") {
        if (id === selection.from) return setSelection(null);
        const reachable = fortifyChoiceFrom(selection.from)?.reachable.find(
          (option) => option.to === id,
        );
        if (reachable) {
          return setSelection({
            ...selection,
            to: id,
            armies: Math.min(1, reachable.maxArmies),
          });
        }
      }

      if (intent === "attack" && attackAction?.choices.some((choice) => choice.from === id)) {
        if (combat && shouldDismissAttackSummary(combat, attackAction, id)) {
          setDismissedCombatId(combat.attackId);
        }
        setSelection({ kind: "attack", from: id, dice: 1 });
        return;
      }
      if (intent === "fortify" && fortifyChoiceFrom(id)) {
        if (combat?.status === "resolved") setDismissedCombatId(combat.attackId);
        setSelection({ kind: "fortify", from: id, armies: 1 });
      }
    },
    [
      occupyAction,
      combat,
      reinforceAction,
      adjustReinforcement,
      selection,
      intent,
      attackAction,
      attackChoicesFrom,
      fortifyChoiceFrom,
    ],
  );

  // ---- derived view state ---------------------------------------------------
  const activePlayer = playerById.get(board?.game.activePlayerId ?? "");
  const winner = playerById.get(board?.game.winnerId ?? "");
  // Choosing a fortification is still canonically legal from `attack`; presenting
  // it as the active section does not advance or rewind the aggregate.
  const presentedPhase =
    board?.game.phase === "attack" && decision?.mode === "active-turn" && intent === "fortify"
      ? "fortify"
      : board?.game.phase;
  // The live route is the attack still being decided. Once its own throw has landed
  // on the map, the resolved trace draws the same two countries along the same curve,
  // so keeping the dashed route as well hides it underneath the solid one and says
  // nothing the trace does not already say. The attack still awaiting occupation is
  // announced by the combat card, not by a second arrow.
  const tracedAttackId = visibleTrace?.attackId ?? null;
  const route: { from: string; to: string } | null =
    selection?.kind === "attack" && selection.to
      ? { from: selection.from, to: selection.to }
      : visibleCombat &&
          visibleCombat.status !== "resolved" &&
          visibleCombat.attackId !== tracedAttackId
        ? { from: visibleCombat.from, to: visibleCombat.to }
        : null;

  const mapTerritories: MapTerritory[] = useMemo(
    () =>
      (board?.territories ?? []).map((territory) => ({
        id: territory.id,
        name: territory.name,
        continentId: territory.continentId,
        ownerId: territory.ownerId,
        armies: territory.armies,
        hexIds: territory.hexIds,
        labelAnchor: territory.labelAnchor,
      })),
    [board?.territories],
  );

  if (!board) {
    return (
      <main className="game-shell">
        <TopBar gameId={gameId}>
          <SyncPill status={live.status} offset={live.streamOffset} error={live.error} />
        </TopBar>
        <section className="honest-empty">
          <div className="loader-ring" />
          <h1>Connecting to the board stream</h1>
          <p>No placeholder armies here — the map appears when the projection’s state arrives.</p>
          {live.error && <div className="notice error">{live.error}</div>}
        </section>
      </main>
    );
  }

  if (board.game.status === "lobby") {
    return (
      <main className="game-shell">
        <TopBar gameId={gameId}>
          <SyncPill
            status={live.status}
            offset={board.meta?.sourceThroughOffset ?? live.streamOffset}
            error={live.error}
          />
        </TopBar>
        <Lobby
          players={board.players}
          hostPlayerId={board.game.hostPlayerId}
          mapSeed={board.game.mapSeed}
          identity={identity}
          spectating={spectating}
          name={props.name}
          busy={busy}
          agentSeats={agentSeats}
          onName={props.onName}
          onJoin={joinGame}
          onStart={startGame}
          onAddAgent={addAgentSeat}
          onCopy={props.onCopyInvite}
        />
        {notice && (
          <div className="notice" role="status">
            {notice}
          </div>
        )}
      </main>
    );
  }

  // Whose turn it is, not whether a decision happens to be open: a pending defence
  // empties the legal actions without handing the turn to anybody else.
  const yourTurn = activePlayer !== undefined && activePlayer.id === selfId;
  const statusLine = seatStatusLabel({
    spectating,
    mode: decision?.mode ?? null,
    activePlayerName: activePlayer?.name ?? "the next player",
    finished: board.game.status === "finished",
    winnerName: winner?.name,
    yourTurn,
  });

  const detailId = detailTerritoryId(hoveredId, focusedId);
  const focused = detailId ? territoryById.get(detailId) : undefined;

  return (
    <main className="game-shell current">
      <MatchBar
        gameId={gameId}
        round={board.game.round}
        activePlayer={activePlayer}
        phase={board.game.phase}
        finished={board.game.status === "finished"}
        winnerName={winner?.name}
        selfId={selfId}
      >
        {spectating && <span className="spectating-badge">Spectating live</span>}
        <SyncPill
          status={live.status}
          offset={board.meta?.sourceThroughOffset ?? live.streamOffset}
          error={live.error}
        />
      </MatchBar>

      <div className="game-layout-v3">
        <TurnColumn
          status={board.game.status}
          turn={board.turn}
          phase={presentedPhase}
          activePlayer={activePlayer}
          names={names}
          statusLine={statusLine}
          yourTurn={yourTurn}
          selfId={selfId}
          fortifyAction={
            !spectating && intent === "fortify" ? (
              <button
                className="phase-back"
                disabled={interactionBusy}
                onClick={() => {
                  setIntent("attack");
                  setSelection(null);
                }}
              >
                ← Back
              </button>
            ) : undefined
          }
          combatCard={
            visibleCombat && (
              <CombatCard
                key={visibleCombat.attackId}
                combat={visibleCombat}
                names={names}
                colorOf={colorOf}
                controllerOf={controllerOf}
                selfId={selfId}
                mode={decision?.mode ?? null}
                now={now}
                defenseWindowMs={
                  visibleCombat.declaredAt !== undefined &&
                  visibleCombat.defenseDeadlineAt !== undefined
                    ? visibleCombat.defenseDeadlineAt - visibleCombat.declaredAt
                    : RULES.defenseTimeoutMs
                }
                reveal={reveal}
                busy={interactionBusy}
                onRollDefense={() => {
                  if (!defenseAction || !identity) return;
                  void submit(
                    { type: "roll-defense", attackId: defenseAction.attackId },
                    `${identity.playerId}:defense:${defenseAction.attackId}`,
                  );
                }}
                attackAgain={
                  repeatAttack
                    ? {
                        maxAttackerDice:
                          attackAction?.choices.find(
                            (choice) =>
                              choice.from === visibleCombat.from && choice.to === visibleCombat.to,
                          )?.maxAttackerDice ?? repeatAttack.attackerDice,
                        onSubmit: (attackerDice) => {
                          const action = attackAgainAction(
                            attackAction,
                            visibleCombat,
                            attackerDice,
                          );
                          if (!action) return;
                          setSelection(null);
                          void submit(action);
                        },
                      }
                    : undefined
                }
              />
            )
          }
          controls={
            board.game.status === "finished" ? (
              <VictoryCard winnerName={winner?.name} round={board.game.round} />
            ) : spectating ? null : (
              <PhaseControls
                names={names}
                busy={interactionBusy}
                selection={selection}
                attackPhase={board.game.phase === "attack" && Boolean(skipFortificationsAction)}
                setSelection={setSelection}
                intent={intent}
                setIntent={setIntent}
                reinforceAction={reinforceAction}
                pendingReinforcements={pendingReinforcements}
                adjustReinforcement={adjustReinforcement}
                finishReinforcements={() => void finishReinforcements()}
                attackAction={attackAction}
                fortifyAction={fortifyAction}
                skipFortificationsAction={skipFortificationsAction}
                occupyAction={occupyAction}
                occupyArmies={occupyArmies}
                setOccupyArmies={setOccupyArmies}
                submit={submit}
                fortifyChoiceFrom={fortifyChoiceFrom}
              />
            )
          }
        />

        <section className="map-column">
          <HexMap
            hexes={board.hexes}
            territories={mapTerritories}
            continents={board.continents}
            colorOf={colorOf}
            ownerNameOf={(id) => (id ? names.player(id) : "nobody")}
            stateOf={stateOf}
            actionable={actionable}
            focusedId={focusedId}
            onSelect={onSelect}
            onDecrement={reinforceAction ? (id) => adjustReinforcement(id, -1) : undefined}
            pendingReinforcements={pendingReinforcements}
            onFocus={setFocusedId}
            onHover={setHoveredId}
            route={route}
            trace={visibleTrace}
            zoom={zoom}
            pan={pan}
            onView={(next) => {
              setZoom(next.zoom);
              setPan(next.pan);
            }}
          >
            <MapControls
              onZoomIn={() => setZoom((value) => Math.min(2.4, Number((value + 0.2).toFixed(2))))}
              onZoomOut={() => setZoom((value) => Math.max(0.6, Number((value - 0.2).toFixed(2))))}
              onReset={() => {
                setZoom(1);
                setPan({ x: 0, y: 0 });
              }}
              onPanUp={() => setPan((value) => ({ ...value, y: value.y + 40 }))}
              onPanLeft={() => setPan((value) => ({ ...value, x: value.x + 40 }))}
              onPanRight={() => setPan((value) => ({ ...value, x: value.x - 40 }))}
              onPanDown={() => setPan((value) => ({ ...value, y: value.y - 40 }))}
            />
          </HexMap>

          {focused && (
            <section className="details-card">
              <div>
                <span className="section-label">
                  {names.continent(focused.continentId)}
                  {/* Continents no longer all pay the same bonus, so what this one is
                      worth belongs next to its name and not only in the standings. */}
                  {` · +${continentById.get(focused.continentId)?.reinforcementBonus ?? 0} per turn`}
                  {continentById.get(focused.continentId)?.controllerId
                    ? ` · held by ${names.player(continentById.get(focused.continentId)?.controllerId)}`
                    : ""}
                </span>
                <h3>{focused.name}</h3>
                <p>{countryLabel(focused, names.player(focused.ownerId))}</p>
                <small>
                  {terrainMix((hexesByTerritory.get(focused.id) ?? []).map((hex) => hex.terrain))}
                  {" · borders "}
                  {focused.adjacentTerritoryIds.map((id) => names.territory(id)).join(", ")}
                </small>
              </div>
            </section>
          )}

          {notice && (
            <div className="notice" role="status">
              {notice}
            </div>
          )}
        </section>

        <StatusColumn
          players={board.players}
          continents={board.continents}
          territories={board.territories}
          moves={board.moves}
          names={names}
          colorOf={colorOf}
          activePlayerId={board.game.activePlayerId}
          selfId={selfId}
          footer={
            <div className="session-card">
              <button className="ghost" onClick={() => void props.onCopyInvite()}>
                Copy game link
              </button>
              {spectating ? (
                <small>Spectating live</small>
              ) : (
                <button className="text-button" onClick={() => props.onIdentity(null)}>
                  Leave player seat
                </button>
              )}
            </div>
          }
        />
      </div>
    </main>
  );
}

// ---------------------------------------------------------------------------
// Phase controls: source → target → amount → confirm
// ---------------------------------------------------------------------------

function Stepper(props: {
  label: string;
  value: number;
  min: number;
  max: number;
  onChange(value: number): void;
}) {
  return (
    <div className="stepper" role="group" aria-label={props.label}>
      <button
        onClick={() => props.onChange(Math.max(props.min, props.value - 1))}
        disabled={props.value <= props.min}
        aria-label={`${props.label}: one fewer`}
      >
        −
      </button>
      <b aria-live="polite">{props.value}</b>
      <button
        onClick={() => props.onChange(Math.min(props.max, props.value + 1))}
        disabled={props.value >= props.max}
        aria-label={`${props.label}: one more`}
      >
        +
      </button>
      <small>
        {props.min}–{props.max}
      </small>
    </div>
  );
}

interface PhaseControlsProps {
  names: NameLookup;
  busy: boolean;
  selection: Selection;
  attackPhase: boolean;
  setSelection(next: Selection): void;
  intent: Intent;
  setIntent(next: Intent): void;
  reinforceAction?: Extract<LegalAction, { type: "reinforce" }>;
  pendingReinforcements: PendingReinforcements;
  adjustReinforcement(territoryId: string, delta: 1 | -1): void;
  finishReinforcements(): void;
  attackAction?: Extract<LegalAction, { type: "declare-attack" }>;
  fortifyAction?: Extract<LegalAction, { type: "fortify" }>;
  skipFortificationsAction?: Extract<LegalAction, { type: "skip-fortifications" }>;
  occupyAction?: Extract<LegalAction, { type: "occupy-territory" }>;
  occupyArmies: number | null;
  setOccupyArmies(value: number): void;
  submit(action: PlayAction): Promise<boolean>;
  fortifyChoiceFrom(
    from: string,
  ): { reachable: Array<{ to: string; maxArmies: number }> } | undefined;
}

/**
 * The controls for the phase in progress, and nothing else.
 *
 * The phase section around these controls already says what the phase is for and
 * whose it is, so nothing here repeats that: this renders the state of the move
 * being composed — source, target, amount, confirm — plus the compulsory occupation
 * card. The optional fortification can also be skipped from its own phase.
 */
export function PhaseControls(props: PhaseControlsProps): ReactNode {
  const { names, selection } = props;

  // Occupation is compulsory: no other affordance is offered until it commits.
  if (props.occupyAction) {
    const action = props.occupyAction;
    const armies = props.occupyArmies ?? action.minArmies;
    return (
      <section className="controls-card required">
        <span className="section-label">Occupy {names.territory(action.to)}</span>
        <p>
          Move armies forward from {names.territory(action.from)}. At least the dice you attacked
          with must advance, and one army must remain behind.
        </p>
        <Stepper
          label="Armies to move"
          value={armies}
          min={action.minArmies}
          max={action.maxArmies}
          onChange={props.setOccupyArmies}
        />
        <button
          className="primary"
          disabled={props.busy}
          onClick={() =>
            void props.submit({
              type: "occupy-territory",
              attackId: action.attackId,
              armies,
            })
          }
        >
          Occupy with {armies}
        </button>
      </section>
    );
  }

  if (props.reinforceAction) {
    return (
      <ReinforcementPlacement
        action={props.reinforceAction}
        names={names}
        pending={props.pendingReinforcements}
        busy={props.busy}
        onAdjust={props.adjustReinforcement}
        onFinish={props.finishReinforcements}
      />
    );
  }

  if (
    props.attackPhase ||
    props.attackAction ||
    props.fortifyAction ||
    props.skipFortificationsAction
  ) {
    return (
      <section className="controls-card">
        {selection?.kind === "attack" && props.attackAction ? (
          selection.to ? (
            <>
              <p>
                <b>{names.territory(selection.from)}</b> attacks{" "}
                <b>{names.territory(selection.to)}</b>
              </p>
              <Stepper
                label="Attacker dice"
                value={selection.dice}
                min={1}
                max={
                  props.attackAction.choices.find(
                    (choice) => choice.from === selection.from && choice.to === selection.to,
                  )?.maxAttackerDice ?? 1
                }
                onChange={(dice) => props.setSelection({ ...selection, dice })}
              />
              <div className="controls-actions">
                <button
                  className="primary"
                  disabled={props.busy}
                  onClick={() =>
                    void props
                      .submit({
                        type: "declare-attack",
                        from: selection.from,
                        to: selection.to!,
                        attackerDice: selection.dice,
                      })
                      .then((accepted) => accepted && props.setSelection(null))
                  }
                >
                  Declare attack
                </button>
                <button onClick={() => props.setSelection({ ...selection, to: undefined })}>
                  Change target
                </button>
              </div>
            </>
          ) : (
            <p>
              <b>{names.territory(selection.from)}</b> selected — choose a highlighted enemy
              neighbour.
            </p>
          )
        ) : selection?.kind === "fortify" ? (
          selection.to ? (
            <>
              <p>
                Move armies <b>{names.territory(selection.from)}</b> →{" "}
                <b>{names.territory(selection.to)}</b> through your own countries.
              </p>
              <Stepper
                label="Armies to move"
                value={selection.armies}
                min={1}
                max={
                  props
                    .fortifyChoiceFrom(selection.from)
                    ?.reachable.find((option) => option.to === selection.to)?.maxArmies ?? 1
                }
                onChange={(armies) => props.setSelection({ ...selection, armies })}
              />
              <div className="controls-actions">
                <button
                  className="primary"
                  disabled={props.busy}
                  onClick={() =>
                    void props
                      .submit({
                        ...canonicalFortifyAction(selection.from, selection.to!, selection.armies),
                      })
                      .then((accepted) => accepted && props.setSelection(null))
                  }
                >
                  Fortify with {selection.armies}
                </button>
                <button onClick={() => props.setSelection({ ...selection, to: undefined })}>
                  Change destination
                </button>
              </div>
            </>
          ) : (
            <p>
              <b>{names.territory(selection.from)}</b> selected — every country reachable through
              your own is highlighted.
            </p>
          )
        ) : (
          <p className="muted">
            {props.intent === "attack"
              ? "Pick a highlighted country to attack from."
              : props.fortifyAction
                ? "Pick a highlighted country to make your one fortification."
                : "No fortification is available."}
          </p>
        )}

        {props.intent === "fortify" && props.skipFortificationsAction && (
          <button
            className="skip-fortifications"
            disabled={props.busy}
            onClick={() =>
              void props
                .submit({ type: "skip-fortifications" })
                .then((accepted) => accepted && props.setSelection(null))
            }
          >
            Skip fortifications
          </button>
        )}

        {props.intent === "attack" && (
          <button
            className="fortify-next"
            disabled={props.busy}
            onClick={() => {
              props.setIntent("fortify");
              props.setSelection(null);
            }}
          >
            Fortify →
          </button>
        )}
      </section>
    );
  }

  return null;
}
