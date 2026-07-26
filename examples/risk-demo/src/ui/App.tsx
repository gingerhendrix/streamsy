import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  type CommandAck,
  type CreateGameResponse,
  type DecisionResponse,
  type GameResponse,
  type JoinGameResponse,
  type PlayAction,
  type PlayCommandRequest,
} from "../application/api.ts";
import type { GameStatus } from "../domain/aggregate.ts";
import type { LegalAction } from "../application/legal-actions.ts";
import { TERRITORIES } from "../domain/map.ts";
import type { ProjectedMove, ProjectedPlayer, ProjectedTerritory } from "../board/projection.ts";
import { useRiskBoardStream } from "./board-stream-db.ts";
import { GameV2Screen, type AgentSeat } from "./game-v2.tsx";
import {
  COLORS,
  PlayerFields,
  STORAGE_KEY,
  SyncPill,
  TopBar,
  acknowledgementNotice,
  api,
  errorMessage,
  gameFromUrl,
  isError,
  loadIdentity,
  playerRoleLabel,
  rendererForGame,
  shortOffset,
  type Identity,
} from "./shared.tsx";

export { acknowledgementNotice, playerRoleLabel, rendererForGame, shortOffset } from "./shared.tsx";

const POSITIONS: Record<string, { x: number; y: number }> = {
  alpha: { x: 15, y: 25 },
  bravo: { x: 50, y: 18 },
  charlie: { x: 84, y: 29 },
  delta: { x: 18, y: 72 },
  echo: { x: 52, y: 68 },
  foxtrot: { x: 84, y: 75 },
};

export function didGameStatusChange(previous: GameStatus | null, current: GameStatus): boolean {
  return previous !== null && previous !== current;
}

function moveText(move: ProjectedMove, players: ProjectedPlayer[]): string {
  const player = players.find((item) => item.id === move.playerId)?.name ?? "A player";
  switch (move.kind) {
    case "GameCreated":
      return `${player} opened the lobby`;
    case "PlayerJoined":
      return `${player} joined the game`;
    case "GameStarted":
      return "Territories dealt — the campaign begins";
    case "ArmiesReinforced":
      return `${player} reinforced ${move.territoryId} with ${move.armies}`;
    case "AttackResolved":
      return `${player} attacked ${move.from} → ${move.to}`;
    case "ArmiesFortified":
      return `${player} moved ${move.armies} armies ${move.from} → ${move.to}`;
    case "TurnEnded":
      return `${player} ended their turn`;
    case "PlayerEliminated":
      return `${player} was eliminated`;
    case "GameWon":
      return `${player} conquered the map`;
  }
}

function attackResult(move: ProjectedMove): string | null {
  if (!move.attackerRolls || !move.defenderRolls) return null;
  const losses = `${move.attackerLosses ?? 0} attacker / ${move.defenderLosses ?? 0} defender lost`;
  return `⚄ ${move.attackerRolls.join(" · ")} vs ${move.defenderRolls.join(" · ")} · ${losses}${move.territoryCaptured ? " · captured" : ""}`;
}

export function App() {
  const [identity, setIdentity] = useState<Identity | null>(() => loadIdentity());
  const [gameId, setGameId] = useState(() => loadIdentity()?.gameId ?? gameFromUrl());
  const [joinId, setJoinId] = useState(() => gameFromUrl());
  const [game, setGame] = useState<GameResponse | null>(null);
  const [decision, setDecision] = useState<DecisionResponse | null>(null);
  const [name, setName] = useState("Player");
  const [color, setColor] = useState(COLORS[0]!);
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [initialAgentSeats, setInitialAgentSeats] = useState<AgentSeat[]>([]);
  const previousStatus = useRef<GameStatus | null>(null);
  // The renderer is chosen from the game's canonical ruleset, never inferred from
  // missing rows (design spec §11) — so only a v1 game opens the v1 board stream.
  const renderer = rendererForGame(game);
  const live = useRiskBoardStream(
    renderer === "risk-demo-v1" ? (game?.boardStreamId ?? null) : null,
  );
  const board = live.rows;

  useEffect(() => {
    const current = board?.game.status;
    if (!current) return;
    if (didGameStatusChange(previousStatus.current, current)) setNotice("");
    previousStatus.current = current;
  }, [board?.game.status]);

  useEffect(() => {
    if (identity || !board) return;
    const used = new Set(board.players.map((player) => player.color.toLowerCase()));
    if (used.has(color.toLowerCase())) {
      setColor(COLORS.find((candidate) => !used.has(candidate.toLowerCase())) ?? COLORS[0]!);
    }
  }, [board, color, identity]);

  const persist = useCallback((next: Identity | null) => {
    setIdentity(next);
    if (next) sessionStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    else sessionStorage.removeItem(STORAGE_KEY);
  }, []);

  const openGame = useCallback((nextGameId: string) => {
    setGameId(nextGameId);
    setJoinId(nextGameId);
    const url = new URL(window.location.href);
    if (nextGameId) url.searchParams.set("game", nextGameId);
    else url.searchParams.delete("game");
    window.history.replaceState({}, "", url);
  }, []);

  const refreshGame = useCallback(async () => {
    if (!gameId) {
      setGame(null);
      return;
    }
    const result = await api<GameResponse>("GET", `/v1/games/${gameId}`);
    if (result.status === 200 && !isError(result.body)) setGame(result.body);
    else {
      setGame(null);
      setNotice(errorMessage(result.body, "Game not found."));
    }
  }, [gameId]);

  useEffect(() => {
    void refreshGame();
    if (!gameId) return;
    let timer = 0;
    let stopped = false;
    const scheduleGenerationCheck = () => {
      timer = window.setTimeout(async () => {
        await refreshGame();
        if (!stopped) scheduleGenerationCheck();
      }, 12_000);
    };
    const onFocus = () => void refreshGame();
    window.addEventListener("focus", onFocus);
    scheduleGenerationCheck();
    return () => {
      stopped = true;
      window.clearTimeout(timer);
      window.removeEventListener("focus", onFocus);
    };
  }, [gameId, refreshGame]);

  useEffect(() => {
    if (!identity || board?.game.activePlayerId !== identity.playerId) {
      setDecision(null);
      return;
    }
    const aborter = new AbortController();
    void api<DecisionResponse>("GET", `/v1/games/${identity.gameId}/decision`, {
      token: identity.token,
    }).then((result) => {
      if (!aborter.signal.aborted && result.status === 200 && !isError(result.body)) {
        setDecision(result.body);
      }
    });
    return () => aborter.abort();
  }, [identity, board?.game.activePlayerId, board?.meta?.sourceThroughOffset]);

  const createGame = async () => {
    setBusy(true);
    const result = await api<CreateGameResponse>("POST", "/v1/games", {
      body: { name, color },
    });
    setBusy(false);
    if (result.status !== 201 || isError(result.body)) {
      setNotice(errorMessage(result.body, "Could not create the game."));
      return;
    }
    persist({
      gameId: result.body.game.id,
      playerId: result.body.player.id,
      token: result.body.capability,
      role: "host",
    });
    openGame(result.body.game.id);
    setNotice("Lobby created. Share the invite link with another player.");
  };

  const createAgentGame = async () => {
    setBusy(true);
    const created = await api<CreateGameResponse>("POST", "/v1/games", {
      body: { name: "Agent 1", color: "#8b5cf6", controller: "agent" },
    });
    if (created.status !== 201 || isError(created.body)) {
      setBusy(false);
      setNotice(errorMessage(created.body, "Could not create the agent game."));
      return;
    }

    const firstSeat: AgentSeat = {
      playerId: created.body.player.id,
      name: created.body.player.name,
      instructions: created.body.agentInstructions ?? "Agent instructions were not returned.",
    };
    persist({
      gameId: created.body.game.id,
      playerId: created.body.player.id,
      token: created.body.capability,
      role: "host",
    });
    setInitialAgentSeats([firstSeat]);
    openGame(created.body.game.id);

    const joined = await api<JoinGameResponse>(
      "POST",
      `/v1/games/${created.body.game.id}/players`,
      {
        body: { name: "Agent 2", color: "#22c1a5", controller: "agent" },
      },
    );
    setBusy(false);
    if (joined.status !== 201 || isError(joined.body)) {
      setNotice(
        `${errorMessage(joined.body, "Could not open the second agent seat.")} You can add it from the lobby.`,
      );
      return;
    }
    setInitialAgentSeats([
      firstSeat,
      {
        playerId: joined.body.player.id,
        name: joined.body.player.name,
        instructions: joined.body.agentInstructions ?? "Agent instructions were not returned.",
      },
    ]);
    setNotice(
      "Agent-versus-agent lobby created. Copy each instruction block, then start the game.",
    );
  };

  const joinGame = async () => {
    const target = joinId.trim();
    if (!target) return;
    setBusy(true);
    const result = await api<JoinGameResponse>("POST", `/v1/games/${target}/players`, {
      body: { name, color },
    });
    setBusy(false);
    if (result.status !== 201 || isError(result.body)) {
      setNotice(errorMessage(result.body, "Could not join the game."));
      return;
    }
    persist({
      gameId: target,
      playerId: result.body.player.id,
      token: result.body.capability,
      role: "player",
    });
    openGame(target);
    await refreshGame();
    setNotice(`Joined as ${result.body.player.name}.`);
  };

  const startGame = async () => {
    if (!identity) return;
    setBusy(true);
    const result = await api<CommandAck>("POST", `/v1/games/${identity.gameId}/start`, {
      token: identity.token,
      body: {},
    });
    if (result.status === 200 && !isError(result.body)) {
      try {
        if (!live.session) throw new Error("Board session is not connected.");
        await live.session.awaitTxId(result.body.txid);
        setNotice("Game started — watch the live board deal territories.");
      } catch {
        setNotice("Game started, but the live board is still catching up.");
      }
    } else {
      setNotice(errorMessage(result.body, "Could not start the game."));
    }
    setBusy(false);
    await refreshGame();
  };

  const submit = async (action: PlayAction) => {
    if (!identity || !decision) return;
    setBusy(true);
    const body: PlayCommandRequest = {
      commandId: `${identity.playerId}:${decision.turn.id}:${crypto.randomUUID().slice(0, 8)}`,
      turnId: decision.turn.id,
      action,
    };
    const result = await api<CommandAck>("POST", `/v1/games/${identity.gameId}/commands`, {
      token: identity.token,
      body,
    });
    if (result.status === 200 && !isError(result.body)) {
      try {
        if (!live.session) throw new Error("Board session is not connected.");
        await live.session.awaitTxId(result.body.txid);
        setNotice(acknowledgementNotice(action.type));
      } catch {
        setNotice(`${acknowledgementNotice(action.type)} Live board still catching up.`);
      }
    } else {
      setNotice(errorMessage(result.body, "Move rejected."));
    }
    setBusy(false);
    await refreshGame();
  };

  const copyInvite = async () => {
    const url = new URL(window.location.href);
    url.searchParams.set("game", gameId);
    await navigator.clipboard.writeText(url.toString());
    setNotice("Invite link copied.");
  };

  const playerById = useMemo(
    () => new Map((board?.players ?? []).map((player) => [player.id, player])),
    [board?.players],
  );
  const reinforce = decision?.legalActions.find(
    (action): action is Extract<LegalAction, { type: "reinforce" }> => action.type === "reinforce",
  );
  const active = board ? playerById.get(board.game.activePlayerId ?? "") : undefined;
  const winner = board ? playerById.get(board.game.winnerId ?? "") : undefined;

  if (!gameId) {
    return (
      <main className="landing-shell">
        <section className="hero-card">
          <div className="eyebrow">
            <span className="live-dot" /> Streamsy live state demo
          </div>
          <h1>Risk, resolved as a stream.</h1>
          <p className="hero-copy">
            A procedurally generated hex map. Recorded dice. A board that rebuilds and synchronises
            live from a durable projection.
          </p>
          <PlayerFields name={name} color={color} onName={setName} onColor={setColor} />
          <button className="primary big" onClick={createGame} disabled={busy}>
            {busy ? "Creating…" : "Create a game"}
          </button>
          <button onClick={createAgentGame} disabled={busy}>
            {busy ? "Creating…" : "Create agent vs agent game"}
          </button>
          <div className="join-row">
            <input
              value={joinId}
              onChange={(event) => setJoinId(event.target.value)}
              placeholder="Game ID"
            />
            <button onClick={() => openGame(joinId.trim())} disabled={!joinId.trim()}>
              View lobby
            </button>
          </div>
          {notice && (
            <div className="notice" role="status">
              {notice}
            </div>
          )}
        </section>
        <aside className="stream-story" aria-label="How the demo works">
          <div className="stream-line" />
          <StoryStep
            number="01"
            title="Commands"
            copy="Moves arrive over the typed REST command API."
          />
          <StoryStep
            number="02"
            title="Projection"
            copy="Canonical events update a replay-safe Durable State stream."
          />
          <StoryStep
            number="03"
            title="Live board"
            copy="The browser long-polls Streamsy and applies change messages."
          />
        </aside>
      </main>
    );
  }

  if (renderer === "risk-demo-v2" && game) {
    return (
      <GameV2Screen
        gameId={gameId}
        game={game}
        identity={identity}
        onIdentity={persist}
        refreshGame={refreshGame}
        name={name}
        color={color}
        onName={setName}
        onColor={setColor}
        onCopyInvite={copyInvite}
        initialAgentSeats={initialAgentSeats}
      />
    );
  }

  return (
    <main className="game-shell">
      <TopBar gameId={gameId}>
        <SyncPill
          status={live.status}
          offset={board?.meta?.sourceThroughOffset ?? live.streamOffset}
          error={live.error}
        />
      </TopBar>

      {!board ? (
        <section className="honest-empty">
          <div className="loader-ring" />
          <h1>Connecting to the board stream</h1>
          <p>
            No placeholder armies here—the map appears when the projection’s first state arrives.
          </p>
          {live.error && <div className="notice error">{live.error}</div>}
        </section>
      ) : (
        <div className="game-layout">
          <section className="main-column">
            <div className="turn-banner">
              <div>
                <span className="eyebrow">
                  Round {board.game.round || "—"} · {board.game.status}
                </span>
                <h1>
                  {winner
                    ? `${winner.name} wins the map`
                    : board.game.status === "lobby"
                      ? "Gather your players"
                      : `${active?.name ?? "Player"} · ${board.game.phase}`}
                </h1>
              </div>
              {active && (
                <span
                  className="active-chip"
                  style={{ "--player": active.color } as React.CSSProperties}
                >
                  {active.id === identity?.playerId ? "Your turn" : `${active.name}’s turn`}
                </span>
              )}
            </div>

            {board.game.status === "lobby" ? (
              <Lobby
                players={board.players}
                hostPlayerId={board.game.hostPlayerId}
                identity={identity}
                name={name}
                color={color}
                busy={busy}
                onName={setName}
                onColor={setColor}
                onJoin={joinGame}
                onStart={startGame}
                onCopy={copyInvite}
              />
            ) : (
              <RiskMap
                territories={board.territories}
                players={board.players}
                reinforceIds={new Set(reinforce?.territoryIds ?? [])}
                disabled={busy || !reinforce}
                onTerritory={(id) =>
                  reinforce && void submit({ type: "reinforce", territoryId: id, armies: 1 })
                }
              />
            )}

            {decision && board.game.status === "playing" && (
              <ActionPanel decision={decision} busy={busy} onSubmit={submit} />
            )}
            {notice && (
              <div className="notice" role="status">
                {notice}
              </div>
            )}
          </section>

          <aside className="side-column">
            <Roster
              players={board.players}
              activePlayerId={board.game.activePlayerId}
              selfId={identity?.playerId}
              territories={board.territories}
            />
            <EventFeed moves={board.moves} players={board.players} />
            <div className="session-card">
              <button className="ghost" onClick={copyInvite}>
                Copy game link
              </button>
              {identity ? (
                <button className="text-button" onClick={() => persist(null)}>
                  Leave player seat
                </button>
              ) : board.game.status === "lobby" ? null : (
                <small>Spectating live</small>
              )}
            </div>
          </aside>
        </div>
      )}
    </main>
  );
}

function StoryStep({ number, title, copy }: { number: string; title: string; copy: string }) {
  return (
    <div className="story-step">
      <span>{number}</span>
      <div>
        <h2>{title}</h2>
        <p>{copy}</p>
      </div>
    </div>
  );
}

function Lobby(props: {
  players: ProjectedPlayer[];
  hostPlayerId?: string;
  identity: Identity | null;
  name: string;
  color: string;
  busy: boolean;
  onName(value: string): void;
  onColor(value: string): void;
  onJoin(): void;
  onStart(): void;
  onCopy(): void;
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
              ? "Share the link, then start when everyone has arrived."
              : props.identity
                ? "The host will begin when the lobby is ready."
                : "Choose a name and claim a player seat."}
          </p>
        </div>
        <button className="invite-button" onClick={props.onCopy}>
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
            onName={props.onName}
            onColor={props.onColor}
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
        <button
          className="primary start-button"
          onClick={props.onStart}
          disabled={props.busy || props.players.length < 2}
        >
          {props.players.length < 2 ? "Waiting for 2 players" : "Start game"}
        </button>
      )}
    </section>
  );
}

function RiskMap(props: {
  territories: ProjectedTerritory[];
  players: ProjectedPlayer[];
  reinforceIds: Set<string>;
  disabled: boolean;
  onTerritory(id: string): void;
}) {
  const byId = new Map(props.territories.map((territory) => [territory.id, territory]));
  const colorOf = (ownerId?: string) =>
    props.players.find((player) => player.id === ownerId)?.color ?? "#667085";
  const edges = TERRITORIES.flatMap((territory) =>
    territory.adjacent
      .filter((other) => territory.id < other)
      .map((other) => [territory.id, other] as const),
  );
  return (
    <section className="map-card" aria-label="Risk territory map">
      <div className="map-grid">
        <svg
          className="map-lines"
          viewBox="0 0 100 100"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          {edges.map(([from, to]) => (
            <line
              key={`${from}-${to}`}
              x1={POSITIONS[from]!.x}
              y1={POSITIONS[from]!.y}
              x2={POSITIONS[to]!.x}
              y2={POSITIONS[to]!.y}
            />
          ))}
        </svg>
        {TERRITORIES.map((definition) => {
          const territory = byId.get(definition.id);
          const owner = props.players.find((player) => player.id === territory?.ownerId);
          const actionable = props.reinforceIds.has(definition.id);
          return (
            <button
              key={definition.id}
              className={`territory-button ${actionable ? "actionable" : ""}`}
              style={
                {
                  left: `${POSITIONS[definition.id]!.x}%`,
                  top: `${POSITIONS[definition.id]!.y}%`,
                  "--owner": colorOf(territory?.ownerId),
                } as React.CSSProperties
              }
              disabled={props.disabled || !actionable}
              onClick={() => props.onTerritory(definition.id)}
              aria-label={`${definition.name}, ${territory?.armies ?? 0} armies, owned by ${owner?.name ?? "nobody"}`}
            >
              <span className="territory-name">{definition.name}</span>
              <b>{territory?.armies ?? 0}</b>
              <small>{owner?.name ?? "Unclaimed"}</small>
            </button>
          );
        })}
      </div>
      <div className="map-caption">
        <span>Connected territories share a route</span>
        <span>Click a marked territory to reinforce</span>
      </div>
    </section>
  );
}

function Roster({
  players,
  activePlayerId,
  selfId,
  territories,
}: {
  players: ProjectedPlayer[];
  activePlayerId?: string;
  selfId?: string;
  territories: ProjectedTerritory[];
}) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <h2>Players</h2>
        <span>{players.length}</span>
      </div>
      <div className="roster">
        {players.map((player) => {
          const armies =
            territories
              .filter((territory) => territory.ownerId === player.id)
              .reduce((sum, territory) => sum + territory.armies, 0) + player.remainingArmies;
          return (
            <div
              className={`roster-player ${player.eliminated ? "eliminated" : ""}`}
              key={player.id}
            >
              <span className="player-color" style={{ background: player.color }} />
              <div>
                <b>
                  {player.name}
                  {player.id === selfId ? " (you)" : ""}
                </b>
                <small>
                  {player.eliminated
                    ? "Eliminated"
                    : `${armies} armies · ${territories.filter((territory) => territory.ownerId === player.id).length} territories`}
                </small>
              </div>
              {player.id === activePlayerId && <span className="turn-marker">Turn</span>}
            </div>
          );
        })}
      </div>
    </section>
  );
}

function EventFeed({ moves, players }: { moves: ProjectedMove[]; players: ProjectedPlayer[] }) {
  return (
    <section className="panel feed-panel">
      <div className="panel-heading">
        <h2>Event stream</h2>
        <span>latest</span>
      </div>
      <div className="event-feed">
        {moves.length === 0 ? (
          <p className="muted">Events will appear here as they commit.</p>
        ) : (
          moves.slice(0, 12).map((move) => (
            <article className="event-item" key={move.id}>
              <span className={`event-icon ${move.kind === "AttackResolved" ? "battle" : ""}`}>
                {move.kind === "AttackResolved" ? "⚔" : "◆"}
              </span>
              <div>
                <b>{moveText(move, players)}</b>
                {attackResult(move) && <small>{attackResult(move)}</small>}
                <code>{shortOffset(move.sourceOffset)}</code>
              </div>
            </article>
          ))
        )}
      </div>
    </section>
  );
}

function ActionPanel({
  decision,
  busy,
  onSubmit,
}: {
  decision: DecisionResponse;
  busy: boolean;
  onSubmit(action: PlayAction): Promise<void>;
}) {
  const actions = decision.legalActions;
  return (
    <section className="action-panel">
      <div>
        <span className="section-label">Your move</span>
        <h2>
          {decision.turn.phase === "reinforce"
            ? "Place reinforcements"
            : decision.turn.phase === "attack"
              ? "Choose an attack or fortify"
              : "Finish your turn"}
        </h2>
      </div>
      <div className="action-list">
        {actions.flatMap((action) => {
          if (action.type === "reinforce")
            return [
              <span className="action-hint" key="reinforce">
                Select one of your marked territories · {action.maxArmies} remaining
              </span>,
            ];
          if (action.type === "attack")
            return action.choices.map((choice) => (
              <button
                key={`attack-${choice.from}-${choice.to}`}
                disabled={busy}
                onClick={() =>
                  void onSubmit({ type: "attack", ...choice, attackerDice: choice.maxAttackerDice })
                }
              >
                ⚔ {choice.from} → {choice.to}
                <small>{choice.maxAttackerDice} dice</small>
              </button>
            ));
          if (action.type === "fortify")
            return action.choices.map((choice) => (
              <button
                key={`fortify-${choice.from}-${choice.to}`}
                disabled={busy}
                onClick={() =>
                  void onSubmit({ type: "fortify", from: choice.from, to: choice.to, armies: 1 })
                }
              >
                Move {choice.from} → {choice.to}
                <small>1 army</small>
              </button>
            ));
          return [
            <button
              className="end-turn"
              key="end-turn"
              disabled={busy}
              onClick={() => void onSubmit({ type: "end-turn" })}
            >
              End turn →
            </button>,
          ];
        })}
      </div>
    </section>
  );
}
