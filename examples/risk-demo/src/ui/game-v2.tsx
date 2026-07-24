/**
 * The `risk-demo-v2` playing surface: hex map, phase interactions, current-turn
 * rail, and the defence/dice experience (design spec §8).
 *
 * Interaction follows one model everywhere — **source → target → amount →
 * confirm** — rather than generating a button per legal move. Legality comes from
 * the player-relative decision resource, so this screen never decides what is legal;
 * it decides what is *clickable*, and the canonical command validation has the
 * final word either way.
 *
 * Two v2-specific shapes drive most of the state here:
 *
 *  - **Defence is out-of-turn.** The decision resource is player-relative, so this
 *    screen refetches it on every board change rather than only when the seat is
 *    the active player — a defender is asked to act during someone else's turn.
 *  - **Combat clears canonically.** The projection drops the `combat` row the moment
 *    an attack closes, so the reveal is held client-side and keyed to the board's
 *    source offset: a result already present in the first snapshot after a reload is
 *    shown, not re-animated.
 */

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import type {
  CommandAck,
  DecisionResponseV2,
  GameResponse,
  JoinGameResponse,
  PlayActionV2,
  PlayCommandRequestV2,
} from "../application/api.ts";
import type { LegalActionV2 } from "../application/legal-actions-v2.ts";
import type { ProjectedHexV2, ProjectedMoveV2, ProjectedPlayerV2 } from "../board/projection-v2.ts";
import type { PlayerController } from "../domain/events-v2.ts";
import { RULES_V2 } from "../domain/map-v2.ts";
import { useRiskBoardV2Stream } from "./board-stream-db.ts";
import { combatView } from "./combat-view.ts";
import { HexMap, countryLabel, type MapTerritory, type TerritoryTone } from "./hex-map.tsx";
import {
  agentHarnessCommand,
  moveDetailV2,
  moveTextV2,
  revealPlan,
  seatStatusLabel,
  terrainMix,
  type NameLookup,
  type RevealPlan,
} from "./presentation-v2.ts";
import {
  PlayerFields,
  SyncPill,
  TopBar,
  acknowledgementNotice,
  api,
  errorMessage,
  isError,
  playerRoleLabel,
  type Identity,
} from "./shared.tsx";
import { CombatCard, TurnRail, VictoryCard } from "./turn-rail.tsx";

type Selection =
  | null
  | { kind: "reinforce"; from: string; armies: number }
  | { kind: "attack"; from: string; to?: string; dice: number }
  | { kind: "fortify"; from: string; to?: string; armies: number };

/** Which manoeuvre a click means during the attack phase; both share sources. */
type Intent = "attack" | "fortify";

/** A seat opened for the machine harness, with the capability that drives it. */
interface AgentSeat {
  playerId: string;
  name: string;
  token: string;
}

const AGENT_COLORS = ["#8b5cf6", "#22c1a5", "#d49b35", "#3b82f6"];

export interface GameV2ScreenProps {
  gameId: string;
  game: GameResponse;
  identity: Identity | null;
  onIdentity(next: Identity | null): void;
  refreshGame(): Promise<void>;
  name: string;
  color: string;
  controller: PlayerController;
  onName(value: string): void;
  onColor(value: string): void;
  onController(value: PlayerController): void;
  onCopyInvite(): Promise<void>;
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

function findAction<T extends LegalActionV2["type"]>(
  actions: LegalActionV2[] | undefined,
  type: T,
): Extract<LegalActionV2, { type: T }> | undefined {
  return actions?.find(
    (action): action is Extract<LegalActionV2, { type: T }> => action.type === type,
  );
}

export function GameV2Screen(props: GameV2ScreenProps) {
  const { gameId, identity } = props;
  const live = useRiskBoardV2Stream(props.game.boardStreamId ?? null);
  const board = live.rows;

  const [decision, setDecision] = useState<DecisionResponseV2 | null>(null);
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [selection, setSelection] = useState<Selection>(null);
  const [intent, setIntent] = useState<Intent>("attack");
  const [occupyArmies, setOccupyArmies] = useState<number | null>(null);
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  // A lobby may open more than one agent seat — an agent-versus-agent game is how
  // the demo runs itself to a winner without a human at the keyboard.
  const [agentSeats, setAgentSeats] = useState<AgentSeat[]>([]);
  const reducedMotion = usePrefersReducedMotion();

  const offset = board?.meta?.sourceThroughOffset ?? null;

  // ---- decision (player-relative: a defender acts out of turn) --------------
  useEffect(() => {
    if (!identity) {
      setDecision(null);
      return;
    }
    const controller = new AbortController();
    void api<DecisionResponseV2>("GET", `/v1/games/${gameId}/decision`, {
      token: identity.token,
    }).then((result) => {
      if (controller.signal.aborted) return;
      if (result.status === 200 && !isError(result.body)) setDecision(result.body);
    });
    return () => controller.abort();
  }, [gameId, identity, offset, board?.game.status]);

  const legalActions = decision?.legalActions;
  const reinforceAction = findAction(legalActions, "reinforce");
  const attackAction = findAction(legalActions, "declare-attack");
  const fortifyAction = findAction(legalActions, "fortify");
  const occupyAction = findAction(legalActions, "occupy-territory");
  const defenseAction = findAction(legalActions, "roll-defense");
  const canEndTurn = Boolean(findAction(legalActions, "end-turn"));

  const turnId = board?.turn?.turnId ?? decision?.turn.id ?? "";

  // A new turn is a fresh decision surface; nothing selected carries across.
  useEffect(() => {
    setSelection(null);
    setIntent("attack");
  }, [turnId]);

  useEffect(() => {
    setOccupyArmies(occupyAction ? occupyAction.minArmies : null);
  }, [occupyAction?.attackId, occupyAction?.minArmies]);

  // ---- combat + reveal ------------------------------------------------------
  const combat = useMemo(
    () =>
      board ? combatView({ combat: board.combat, turn: board.turn, moves: board.moves }) : null,
    [board],
  );

  const seenReveals = useRef(new Set<string>());
  const firstOffset = useRef<string | null | undefined>(undefined);
  if (board && firstOffset.current === undefined) firstOffset.current = offset;

  const revealKey = combat ? `${combat.attackId}:${combat.status}` : null;
  const [reveal, setReveal] = useState<RevealPlan>({ mode: "none", durationMs: 0 });
  useEffect(() => {
    if (!revealKey) return;
    // Keyed to the source offset: a throw already present in the first snapshot
    // after a reload is state to display, not an animation to replay.
    const alreadySeen = seenReveals.current.has(revealKey) || offset === firstOffset.current;
    seenReveals.current.add(revealKey);
    setReveal(revealPlan({ reducedMotion, alreadySeen }));
  }, [revealKey, offset, reducedMotion]);

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
    const grouped = new Map<string, ProjectedHexV2[]>();
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
    async (action: PlayActionV2, commandId?: string) => {
      if (!identity || !turnId) return false;
      setBusy(true);
      const body: PlayCommandRequestV2 = {
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
          await live.session.awaitTxId((result.body as CommandAck).txid);
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
        await live.session.awaitTxId(result.body.txid);
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
    const result = await api<JoinGameResponse>("POST", `/v1/games/${gameId}/players`, {
      body: { name: props.name, color: props.color, controller: props.controller },
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

  /** Open an agent seat and hand back the command that drives it. */
  const addAgentSeat = async () => {
    setBusy(true);
    const taken = new Set((board?.players ?? []).map((player) => player.color.toLowerCase()));
    const seat = agentSeats.length + 1;
    const result = await api<JoinGameResponse>("POST", `/v1/games/${gameId}/players`, {
      body: {
        name: `Agent ${seat}`,
        color: AGENT_COLORS.find((color) => !taken.has(color.toLowerCase())) ?? AGENT_COLORS[0],
        controller: "agent",
      },
    });
    setBusy(false);
    if (result.status !== 201 || isError(result.body)) {
      setNotice(errorMessage(result.body, "Could not open an agent seat."));
      return;
    }
    const opened: AgentSeat = {
      playerId: result.body.player.id,
      name: result.body.player.name,
      token: result.body.capability,
    };
    setAgentSeats((seats) => [...seats, opened]);
    setNotice("Agent seat opened — run the harness command to bring it online.");
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

  const actionable = useMemo(() => {
    const ids = new Set<string>();
    if (occupyAction) return ids;
    if (reinforceAction) for (const id of reinforceAction.territoryIds) ids.add(id);
    if (selection?.kind === "attack") {
      ids.add(selection.from);
      for (const choice of attackChoicesFrom(selection.from)) ids.add(choice.to);
    } else if (selection?.kind === "fortify") {
      ids.add(selection.from);
      for (const reachable of fortifyChoiceFrom(selection.from)?.reachable ?? []) {
        ids.add(reachable.to);
      }
    } else if (intent === "attack") {
      for (const choice of attackAction?.choices ?? []) ids.add(choice.from);
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

  const toneOf = useCallback(
    (id: string): TerritoryTone => {
      if (occupyAction) {
        if (id === occupyAction.to) return "target";
        if (id === occupyAction.from) return "selected";
        return "dimmed";
      }
      if (combat && combat.status === "awaiting-defense") {
        if (id === combat.to) return "target";
        if (id === combat.from) return "selected";
        return "dimmed";
      }
      if (selection?.kind === "reinforce") {
        return id === selection.from ? "selected" : actionable.has(id) ? "source" : "idle";
      }
      if (selection?.kind === "attack" || selection?.kind === "fortify") {
        if (id === selection.from) return "selected";
        if (id === selection.to) return "target";
        return actionable.has(id) ? "target" : "dimmed";
      }
      return actionable.has(id) ? "source" : "idle";
    },
    [occupyAction, combat, selection, actionable],
  );

  const onSelect = useCallback(
    (id: string) => {
      setFocusedId(id);
      if (occupyAction || combat?.status === "awaiting-defense") return;

      if (reinforceAction?.territoryIds.includes(id)) {
        setSelection((current) =>
          current?.kind === "reinforce" && current.from === id
            ? null
            : { kind: "reinforce", from: id, armies: 1 },
        );
        return;
      }

      if (selection?.kind === "attack") {
        if (id === selection.from) return setSelection(null);
        const choice = attackChoicesFrom(selection.from).find((option) => option.to === id);
        if (choice) {
          return setSelection({ ...selection, to: id, dice: choice.maxAttackerDice });
        }
      }
      if (selection?.kind === "fortify") {
        if (id === selection.from) return setSelection(null);
        const reachable = fortifyChoiceFrom(selection.from)?.reachable.find(
          (option) => option.to === id,
        );
        if (reachable) {
          return setSelection({ ...selection, to: id, armies: Math.min(1, reachable.maxArmies) });
        }
      }

      if (intent === "attack" && attackAction?.choices.some((choice) => choice.from === id)) {
        setSelection({ kind: "attack", from: id, dice: 1 });
        return;
      }
      if (intent === "fortify" && fortifyChoiceFrom(id)) {
        setSelection({ kind: "fortify", from: id, armies: 1 });
      }
    },
    [
      occupyAction,
      combat,
      reinforceAction,
      selection,
      intent,
      attackAction,
      attackChoicesFrom,
      fortifyChoiceFrom,
    ],
  );

  // ---- derived view state ---------------------------------------------------
  const spectating = !identity || !playerById.has(identity.playerId);
  const activePlayer = playerById.get(board?.game.activePlayerId ?? "");
  const winner = playerById.get(board?.game.winnerId ?? "");
  const route: { from: string; to: string } | null =
    selection?.kind === "attack" && selection.to
      ? { from: selection.from, to: selection.to }
      : combat && combat.status !== "resolved"
        ? { from: combat.from, to: combat.to }
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
        <LobbyV2
          players={board.players}
          hostPlayerId={board.game.hostPlayerId}
          identity={identity}
          name={props.name}
          color={props.color}
          controller={props.controller}
          busy={busy}
          agentSeats={agentSeats}
          origin={window.location.origin}
          gameId={gameId}
          onName={props.onName}
          onColor={props.onColor}
          onController={props.onController}
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

  const statusLine = seatStatusLabel({
    spectating,
    mode: decision?.mode ?? null,
    activePlayerName: activePlayer?.name ?? "the next player",
    finished: board.game.status === "finished",
    winnerName: winner?.name,
  });

  const focused = focusedId ? territoryById.get(focusedId) : undefined;

  return (
    <main className="game-shell v2">
      <TopBar gameId={gameId}>
        <div className="topbar-right">
          {spectating && <span className="spectating-badge">Spectating live</span>}
          <SyncPill
            status={live.status}
            offset={board.meta?.sourceThroughOffset ?? live.streamOffset}
            error={live.error}
          />
        </div>
      </TopBar>

      <div className="game-layout-v2">
        <section className="board-column">
          <HexMap
            hexes={board.hexes}
            territories={mapTerritories}
            continents={board.continents}
            colorOf={colorOf}
            ownerNameOf={(id) => (id ? names.player(id) : "nobody")}
            toneOf={toneOf}
            actionable={actionable}
            focusedId={focusedId}
            onSelect={onSelect}
            onFocus={setFocusedId}
            route={route}
            zoom={zoom}
            pan={pan}
            onView={(next) => {
              setZoom(next.zoom);
              setPan(next.pan);
            }}
          >
            <div className="map-controls">
              <button
                onClick={() => setZoom((value) => Math.min(2.4, Number((value + 0.2).toFixed(2))))}
                aria-label="Zoom in"
              >
                +
              </button>
              <button
                onClick={() => setZoom((value) => Math.max(0.6, Number((value - 0.2).toFixed(2))))}
                aria-label="Zoom out"
              >
                −
              </button>
              <button
                onClick={() => {
                  setZoom(1);
                  setPan({ x: 0, y: 0 });
                }}
                aria-label="Reset the map view"
              >
                Reset
              </button>
              <div className="pan-pad" aria-label="Pan the map" role="group">
                <button onClick={() => setPan((p) => ({ ...p, y: p.y + 40 }))} aria-label="Pan up">
                  ↑
                </button>
                <button
                  onClick={() => setPan((p) => ({ ...p, x: p.x + 40 }))}
                  aria-label="Pan left"
                >
                  ←
                </button>
                <button
                  onClick={() => setPan((p) => ({ ...p, x: p.x - 40 }))}
                  aria-label="Pan right"
                >
                  →
                </button>
                <button
                  onClick={() => setPan((p) => ({ ...p, y: p.y - 40 }))}
                  aria-label="Pan down"
                >
                  ↓
                </button>
              </div>
            </div>
          </HexMap>

          {focused && (
            <section className="details-card" aria-live="polite">
              <div>
                <span className="section-label">
                  {names.continent(focused.continentId)}
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

        <TurnRail
          round={board.game.round}
          status={board.game.status}
          turn={board.turn}
          phase={board.game.phase}
          activePlayer={activePlayer}
          names={names}
          statusLine={statusLine}
          selfId={identity?.playerId}
          combatCard={
            combat && (
              <CombatCard
                combat={combat}
                names={names}
                colorOf={colorOf}
                controllerOf={controllerOf}
                selfId={identity?.playerId}
                mode={decision?.mode ?? null}
                now={now}
                defenseWindowMs={
                  combat.declaredAt !== undefined && combat.defenseDeadlineAt !== undefined
                    ? combat.defenseDeadlineAt - combat.declaredAt
                    : RULES_V2.defenseTimeoutMs
                }
                reveal={reveal}
                busy={busy}
                onRollDefense={() => {
                  if (!defenseAction || !identity) return;
                  void submit(
                    { type: "roll-defense", attackId: defenseAction.attackId },
                    `${identity.playerId}:defense:${defenseAction.attackId}`,
                  );
                }}
              />
            )
          }
          controls={
            board.game.status === "finished" ? (
              <VictoryCard winnerName={winner?.name} round={board.game.round} />
            ) : spectating ? null : (
              <PhaseControls
                names={names}
                busy={busy}
                selection={selection}
                setSelection={setSelection}
                intent={intent}
                setIntent={setIntent}
                reinforceAction={reinforceAction}
                attackAction={attackAction}
                fortifyAction={fortifyAction}
                occupyAction={occupyAction}
                occupyArmies={occupyArmies}
                setOccupyArmies={setOccupyArmies}
                canEndTurn={canEndTurn}
                waiting={Boolean(decision && decision.legalActions.length === 0)}
                submit={submit}
                fortifyChoiceFrom={fortifyChoiceFrom}
              />
            )
          }
          history={<GameHistory moves={board.moves} names={names} />}
          footer={
            <div className="session-card">
              <button className="ghost" onClick={() => void props.onCopyInvite()}>
                Copy game link
              </button>
              {identity ? (
                <button className="text-button" onClick={() => props.onIdentity(null)}>
                  Leave player seat
                </button>
              ) : (
                <small>Spectating live</small>
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
  setSelection(next: Selection): void;
  intent: Intent;
  setIntent(next: Intent): void;
  reinforceAction?: Extract<LegalActionV2, { type: "reinforce" }>;
  attackAction?: Extract<LegalActionV2, { type: "declare-attack" }>;
  fortifyAction?: Extract<LegalActionV2, { type: "fortify" }>;
  occupyAction?: Extract<LegalActionV2, { type: "occupy-territory" }>;
  occupyArmies: number | null;
  setOccupyArmies(value: number): void;
  canEndTurn: boolean;
  waiting: boolean;
  submit(action: PlayActionV2): Promise<boolean>;
  fortifyChoiceFrom(
    from: string,
  ): { reachable: Array<{ to: string; maxArmies: number }> } | undefined;
}

function PhaseControls(props: PhaseControlsProps): ReactNode {
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
            void props.submit({ type: "occupy-territory", attackId: action.attackId, armies })
          }
        >
          Occupy with {armies}
        </button>
      </section>
    );
  }

  if (props.reinforceAction) {
    const action = props.reinforceAction;
    if (selection?.kind !== "reinforce") {
      return (
        <section className="controls-card">
          <span className="section-label">Reinforce</span>
          <p>
            Choose one of your glowing countries. {action.maxArmies}{" "}
            {action.maxArmies === 1 ? "army" : "armies"} left to place.
          </p>
        </section>
      );
    }
    return (
      <section className="controls-card">
        <span className="section-label">Reinforce {names.territory(selection.from)}</span>
        <Stepper
          label="Armies to place"
          value={Math.min(selection.armies, action.maxArmies)}
          min={1}
          max={action.maxArmies}
          onChange={(armies) => props.setSelection({ ...selection, armies })}
        />
        <div className="controls-actions">
          <button
            className="primary"
            disabled={props.busy}
            onClick={() =>
              void props
                .submit({
                  type: "reinforce",
                  territoryId: selection.from,
                  armies: Math.min(selection.armies, action.maxArmies),
                })
                .then((accepted) => accepted && props.setSelection(null))
            }
          >
            Place {selection.armies}
          </button>
          <button onClick={() => props.setSelection(null)}>Cancel</button>
        </div>
      </section>
    );
  }

  if (props.attackAction || props.fortifyAction || props.canEndTurn) {
    return (
      <section className="controls-card">
        <div className="intent-toggle" role="group" aria-label="Manoeuvre">
          <button
            className={props.intent === "attack" ? "selected" : ""}
            aria-pressed={props.intent === "attack"}
            disabled={!props.attackAction}
            onClick={() => {
              props.setIntent("attack");
              props.setSelection(null);
            }}
          >
            Attack
          </button>
          <button
            className={props.intent === "fortify" ? "selected" : ""}
            aria-pressed={props.intent === "fortify"}
            disabled={!props.fortifyAction}
            onClick={() => {
              props.setIntent("fortify");
              props.setSelection(null);
            }}
          >
            Fortify
          </button>
        </div>

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
                        type: "fortify",
                        from: selection.from,
                        to: selection.to!,
                        armies: selection.armies,
                      })
                      .then((accepted) => accepted && props.setSelection(null))
                  }
                >
                  Move {selection.armies}
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
          <p>
            {props.intent === "attack"
              ? "Choose a country with two or more armies bordering an enemy."
              : "Choose a country with two or more armies to move from."}
          </p>
        )}

        {props.canEndTurn && (
          <button
            className="end-turn"
            disabled={props.busy}
            onClick={() => void props.submit({ type: "end-turn" })}
          >
            End turn →
          </button>
        )}
      </section>
    );
  }

  if (props.waiting) {
    return (
      <section className="controls-card">
        <p className="muted">Nothing to do right now — the board is waiting on another player.</p>
      </section>
    );
  }
  return null;
}

// ---------------------------------------------------------------------------
// Supporting panels
// ---------------------------------------------------------------------------

function GameHistory(props: { moves: ProjectedMoveV2[]; names: NameLookup }) {
  return (
    <section className="panel feed-panel">
      <div className="panel-heading">
        <h2>Game history</h2>
        <span>earlier turns</span>
      </div>
      <div className="event-feed">
        {props.moves.length === 0 ? (
          <p className="muted">Moves will appear here as they commit.</p>
        ) : (
          props.moves.slice(0, 14).map((move) => {
            const battle = move.kind === "AttackResolved" || move.kind === "AttackDeclared";
            const detail = moveDetailV2(move, props.names);
            return (
              <article className="event-item" key={move.id}>
                <span className={`event-icon ${battle ? "battle" : ""}`}>{battle ? "⚔" : "◆"}</span>
                <div>
                  <b>{moveTextV2(move, props.names)}</b>
                  {detail && <small>{detail}</small>}
                </div>
              </article>
            );
          })
        )}
      </div>
    </section>
  );
}

function LobbyV2(props: {
  players: ProjectedPlayerV2[];
  hostPlayerId?: string;
  identity: Identity | null;
  name: string;
  color: string;
  controller: PlayerController;
  busy: boolean;
  gameId: string;
  agentSeats: AgentSeat[];
  /** Where this page was served from; the harness needs it as `BASE_URL` (D1). */
  origin: string;
  onName(value: string): void;
  onColor(value: string): void;
  onController(value: PlayerController): void;
  onJoin(): void;
  onStart(): void;
  onAddAgent(): void;
  onCopy(): Promise<void>;
}) {
  const isHost = props.identity?.role === "host";
  return (
    <section className="lobby-card">
      <div className="lobby-top">
        <div>
          <span className="section-label">Lobby · {props.players.length}/4</span>
          <h2>{props.players.length < 2 ? "Waiting for a challenger" : "Ready to deploy"}</h2>
          <p>
            {isHost
              ? "Share the link or open an agent seat, then start when everyone has arrived."
              : props.identity
                ? "The host will begin when the lobby is ready."
                : "Choose a name and claim a player seat."}
          </p>
        </div>
        <button className="invite-button" onClick={() => void props.onCopy()}>
          Copy invite link
        </button>
      </div>
      <div className="lobby-players">
        {props.players.map((player) => (
          <div className="lobby-player" key={player.id}>
            <span className="avatar" style={{ background: player.color }}>
              {player.name.slice(0, 1).toUpperCase()}
            </span>
            <div>
              <b>{player.name}</b>
              <small>
                {playerRoleLabel(props.hostPlayerId, player.id)}
                {player.controller === "agent" ? " · agent" : ""}
                {player.id === props.identity?.playerId ? " · you" : ""}
              </small>
            </div>
            <span className="ready">Ready</span>
          </div>
        ))}
        {Array.from({ length: Math.max(0, 2 - props.players.length) }, (_, index) => (
          <div className="empty-seat" key={index}>
            Open player seat
          </div>
        ))}
      </div>

      {!props.identity && (
        <div className="join-panel">
          <PlayerFields
            name={props.name}
            color={props.color}
            controller={props.controller}
            onName={props.onName}
            onColor={props.onColor}
            onController={props.onController}
          />
          <button
            className="primary"
            onClick={props.onJoin}
            disabled={props.busy || !props.name.trim()}
          >
            {props.busy ? "Joining…" : "Join this game"}
          </button>
        </div>
      )}

      {isHost && (
        <div className="host-actions">
          <button onClick={props.onAddAgent} disabled={props.busy || props.players.length >= 4}>
            Open an agent seat
          </button>
          <button
            className="primary start-button"
            onClick={props.onStart}
            disabled={props.busy || props.players.length < 2}
          >
            {props.players.length < 2 ? "Waiting for 2 players" : "Start game"}
          </button>
        </div>
      )}

      {props.agentSeats.map((seat) => (
        <div className="agent-seat" key={seat.playerId}>
          <b>{seat.name} is seated.</b> Run the harness in a terminal:
          <code>
            {agentHarnessCommand({
              origin: props.origin,
              gameId: props.gameId,
              playerId: seat.playerId,
              token: seat.token,
            })}
          </code>
        </div>
      ))}
    </section>
  );
}
