import { useCallback, useEffect, useState } from "react";

import {
  type AgentSeatResponse,
  type CreateGameResponse,
  type GameResponse,
} from "../application/api.ts";
import { GameScreen, type AgentSeat } from "./game.tsx";
import {
  MAX_SEAT_NAME,
  PlayerFields,
  STORAGE_KEY,
  agentSeatName,
  api,
  errorMessage,
  gameFromUrl,
  gamePath,
  isError,
  loadIdentity,
  type Identity,
} from "./shared.tsx";

export { acknowledgementNotice, playerRoleLabel, shortOffset } from "./shared.tsx";

export function App() {
  const [identity, setIdentity] = useState<Identity | null>(() => {
    const routedGameId = gameFromUrl();
    const savedIdentity = loadIdentity();
    return savedIdentity?.gameId === routedGameId ? savedIdentity : null;
  });
  const [gameId, setGameId] = useState(() => gameFromUrl());
  const [joinId, setJoinId] = useState(() => gameFromUrl());
  const [game, setGame] = useState<GameResponse | null>(null);
  const [name, setName] = useState("Player");
  const [firstAgentName, setFirstAgentName] = useState("Agent 1");
  const [secondAgentName, setSecondAgentName] = useState("Agent 2");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [initialAgentSeats, setInitialAgentSeats] = useState<AgentSeat[]>([]);

  const persist = useCallback((next: Identity | null) => {
    setIdentity(next);
    if (next) sessionStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    else sessionStorage.removeItem(STORAGE_KEY);
  }, []);

  const openGame = useCallback((nextGameId: string) => {
    setGameId(nextGameId);
    setJoinId(nextGameId);
    window.history.pushState({}, "", nextGameId ? gamePath(nextGameId) : "/");
  }, []);

  useEffect(() => {
    const onPopState = () => {
      const routedGameId = gameFromUrl();
      const savedIdentity = loadIdentity();
      setGameId(routedGameId);
      setJoinId(routedGameId);
      setIdentity(savedIdentity?.gameId === routedGameId ? savedIdentity : null);
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  const refreshGame = useCallback(async (): Promise<GameResponse | null> => {
    if (!gameId) {
      setGame(null);
      return null;
    }
    const result = await api<GameResponse>("GET", `/v1/games/${gameId}`);
    if (result.status === 200 && !isError(result.body)) {
      setGame(result.body);
      return result.body;
    }
    setGame(null);
    setNotice(errorMessage(result.body, "Game not found."));
    return null;
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

  const createGame = async () => {
    setBusy(true);
    const result = await api<CreateGameResponse>("POST", "/v1/games", {
      body: { name },
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
    // The host seat is created under the first agent's name because it is that
    // agent's seat from the moment the delegation below commits; the server reuses
    // the existing player's name when a seat is delegated rather than taking one.
    const created = await api<CreateGameResponse>("POST", "/v1/games", {
      body: { name: agentSeatName(firstAgentName, 1) },
    });
    if (created.status !== 201 || isError(created.body)) {
      setBusy(false);
      setNotice(errorMessage(created.body, "Could not create the agent game."));
      return;
    }

    const hostIdentity: Identity = {
      gameId: created.body.game.id,
      playerId: created.body.player.id,
      token: created.body.capability,
      role: "host",
    };
    persist(hostIdentity);
    openGame(created.body.game.id);

    const first = await api<AgentSeatResponse>(
      "POST",
      `/v1/games/${created.body.game.id}/agent-seats`,
      {
        token: created.body.capability,
        body: { playerId: created.body.player.id },
      },
    );
    if (first.status !== 201 || isError(first.body)) {
      setBusy(false);
      setNotice(errorMessage(first.body, "Could not delegate the first agent seat."));
      return;
    }
    // Only now is the seat the agent's: the identity keeps its host capability —
    // starting the game and opening seats still need it — but is marked as a
    // spectator so the playing surface never offers this browser that seat's moves.
    persist({ ...hostIdentity, spectator: true });
    const firstSeat: AgentSeat = {
      playerId: first.body.seat.playerId,
      name: first.body.seat.name,
      instructions: first.body.instructions,
    };
    setInitialAgentSeats([firstSeat]);

    const joined = await api<AgentSeatResponse>(
      "POST",
      `/v1/games/${created.body.game.id}/agent-seats`,
      {
        token: created.body.capability,
        body: { name: agentSeatName(secondAgentName, 2) },
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
        playerId: joined.body.seat.playerId,
        name: joined.body.seat.name,
        instructions: joined.body.instructions,
      },
    ]);
    setNotice(
      "Agent-versus-agent lobby created — you are spectating. Copy each instruction block, then start the game.",
    );
  };

  const copyInvite = async () => {
    const url = new URL(gamePath(gameId), window.location.origin);
    await navigator.clipboard.writeText(url.toString());
    setNotice("Invite link copied.");
  };

  if (!gameId) {
    return (
      <main className="landing-shell">
        <section className="hero-card">
          <div className="eyebrow">
            <span className="live-dot" /> Streamsy live state demo
          </div>
          <h1>Hex Domination, resolved as a stream.</h1>
          <p className="hero-copy">
            A procedurally generated hex map. Recorded dice. A board that rebuilds and synchronises
            live from a durable projection.
          </p>
          <PlayerFields name={name} onName={setName} />
          <button className="primary big" onClick={createGame} disabled={busy}>
            {busy ? "Creating…" : "Create a game"}
          </button>
          {/* Both agents are named up front: the game runs itself to a winner
              while you watch, and a match report of "Agent 1 versus Agent 2" is
              far harder to follow than one between seats you named. */}
          <div className="player-fields compact">
            <label>
              <span>First agent</span>
              <input
                value={firstAgentName}
                maxLength={MAX_SEAT_NAME}
                placeholder="Agent 1"
                onChange={(event) => setFirstAgentName(event.target.value)}
              />
            </label>
            <label>
              <span>Second agent</span>
              <input
                value={secondAgentName}
                maxLength={MAX_SEAT_NAME}
                placeholder="Agent 2"
                onChange={(event) => setSecondAgentName(event.target.value)}
              />
            </label>
          </div>
          <button onClick={createAgentGame} disabled={busy}>
            {busy ? "Creating…" : "Create agent vs agent game"}
          </button>
          <p className="muted">
            You spectate an agent-versus-agent game; both seats play through their own capabilities.
          </p>
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

  if (game) {
    return (
      <GameScreen
        gameId={gameId}
        game={game}
        identity={identity}
        onIdentity={persist}
        refreshGame={refreshGame}
        name={name}
        onName={setName}
        onCopyInvite={copyInvite}
        initialAgentSeats={initialAgentSeats}
      />
    );
  }

  return (
    <main className="game-shell">
      <section className="honest-empty">
        <div className="loader-ring" />
        <h1>Connecting to the game</h1>
        <p>The board appears when the game resource is available.</p>
        {notice && <div className="notice error">{notice}</div>}
      </section>
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
