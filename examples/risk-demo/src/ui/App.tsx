/* oxlint-disable effecttsgo/async-function -- React and the browser own these Promise-native event and lifecycle callbacks; reusable data orchestration remains behind the existing application facade. */
import { useCallback, useEffect, useState } from "react";

import { type CreateGameResponse, type GameResponse } from "../application/api.ts";
import { GameScreen } from "./game.tsx";
import {
  STORAGE_KEY,
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
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);

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
    if (!gameId) return undefined;
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
    // Deliberately nameless: the server issues a provisional seat name and the
    // creator sets the one they want on the muster roll.
    const result = await api<CreateGameResponse>("POST", "/v1/games", { body: {} });
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
    setNotice("Lobby opened. Name your seat, then invite friends or agents.");
  };

  const copyInvite = async () => {
    const url = new URL(gamePath(gameId), window.location.origin);
    await navigator.clipboard.writeText(url.toString());
    setNotice("Invite link copied.");
  };

  if (!gameId) {
    return (
      <Landing
        busy={busy}
        joinId={joinId}
        notice={notice}
        onJoinId={setJoinId}
        onCreate={createGame}
        onOpen={() => openGame(joinId.trim())}
      />
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
        onLeftGame={() => {
          persist(null);
          openGame("");
          setNotice("You left the game.");
        }}
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

/**
 * The front page: an invitation, and one command that acts on it.
 *
 * Deliberately asks for nothing. Everything the previous landing page collected
 * up front — your name, both agents' names, and whether the game was to be
 * agent-versus-agent — is a decision about a roster, and the lobby is the only
 * screen where the roster is visible while you make it. What is left is the
 * invitation, the command, and a way back into a game you were already sent.
 *
 * Pure and browser-free, so it renders under test without a DOM: every piece of
 * routing and session state it needs is passed in.
 */
export function Landing(props: {
  busy: boolean;
  joinId: string;
  notice: string;
  onJoinId: (value: string) => void;
  onCreate: () => void;
  onOpen: () => void;
}) {
  return (
    <main className="landing-shell">
      <section className="hero-card">
        <div className="eyebrow">
          <span className="live-dot" /> Streamsy live state demo
        </div>
        <h1>Can you beat your agent at Hex Domination?</h1>
        <p className="hero-copy">Play with friends or agents.</p>
        <button className="primary big" onClick={props.onCreate} disabled={props.busy}>
          {props.busy ? "Creating…" : "Create a game"}
        </button>
        <div className="join-row">
          <input
            value={props.joinId}
            onChange={(event) => props.onJoinId(event.target.value)}
            placeholder="Game ID"
            aria-label="Game ID"
          />
          <button onClick={props.onOpen} disabled={!props.joinId.trim()}>
            View lobby
          </button>
        </div>
        {props.notice && (
          <div className="notice" role="status">
            {props.notice}
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
