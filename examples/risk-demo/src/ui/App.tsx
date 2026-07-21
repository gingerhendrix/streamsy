/**
 * Minimal playable / spectator Risk board.
 *
 * Board state comes only from the projected board resource (`GET /board`), never
 * from canonical events. When the browser holds the active player's capability it
 * fetches `/decision` and renders the structured legal actions as buttons. Two
 * tabs converge by polling the board on an interval.
 */

import { useCallback, useEffect, useRef, useState } from "react";

// Fixed six-territory layout (matches src/map.ts), positioned as a 3x2 grid.
const LAYOUT: string[][] = [
  ["alpha", "bravo", "charlie"],
  ["delta", "echo", "foxtrot"],
];

interface Board {
  gameId: string;
  sourceThroughOffset: string | null;
  game: {
    status: string;
    phase?: string;
    activePlayerId?: string;
    round: number;
    winnerId?: string;
  };
  players: Array<{
    id: string;
    name: string;
    color: string;
    remainingArmies: number;
    eliminated: boolean;
  }>;
  territories: Array<{ id: string; ownerId?: string; armies: number }>;
}

interface Decision {
  turn: { id: string; phase: string; activePlayerId?: string };
  legalActions: any[];
}

interface Identity {
  gameId: string;
  playerId: string;
  token: string;
  role: string;
}

const STORAGE_KEY = "risk-demo-identity";

function loadIdentity(): Identity | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as Identity) : null;
  } catch {
    return null;
  }
}

async function api(
  method: string,
  path: string,
  opts: { token?: string; body?: unknown } = {},
): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.token) headers.authorization = `Bearer ${opts.token}`;
  const init: RequestInit = { method, headers };
  if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
  const res = await fetch(path, init);
  return { status: res.status, body: await res.json().catch(() => ({})) };
}

export function App() {
  const [identity, setIdentity] = useState<Identity | null>(() => loadIdentity());
  const [board, setBoard] = useState<Board | null>(null);
  const [decision, setDecision] = useState<Decision | null>(null);
  const [message, setMessage] = useState<string>("");
  const [joinId, setJoinId] = useState<string>("");
  const [name, setName] = useState<string>("Player");
  const [color, setColor] = useState<string>("#c0392b");

  const persist = useCallback((next: Identity | null) => {
    setIdentity(next);
    if (next) localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    else localStorage.removeItem(STORAGE_KEY);
  }, []);

  const gameId = identity?.gameId ?? joinId;

  const refresh = useCallback(async () => {
    if (!gameId) return;
    const b = await api("GET", `/v1/games/${gameId}/board`);
    if (b.status === 200) setBoard(b.body as Board);
    if (identity && b.status === 200 && b.body.game.activePlayerId === identity.playerId) {
      const d = await api("GET", `/v1/games/${gameId}/decision`, { token: identity.token });
      if (d.status === 200) setDecision(d.body as Decision);
    } else {
      setDecision(null);
    }
  }, [gameId, identity]);

  const timer = useRef<number | null>(null);
  useEffect(() => {
    void refresh();
    timer.current = window.setInterval(() => void refresh(), 1000);
    return () => {
      if (timer.current !== null) window.clearInterval(timer.current);
    };
  }, [refresh]);

  const createGame = async () => {
    const res = await api("POST", "/v1/games", { body: { name, color } });
    if (res.status === 201) {
      persist({
        gameId: res.body.game.id,
        playerId: res.body.player.id,
        token: res.body.capability,
        role: "host",
      });
      setMessage(`Created game ${res.body.game.id}`);
    } else setMessage(res.body?.error?.message ?? "create failed");
  };

  const joinGame = async () => {
    if (!joinId) return;
    const res = await api("POST", `/v1/games/${joinId}/players`, { body: { name, color } });
    if (res.status === 201) {
      persist({
        gameId: joinId,
        playerId: res.body.player.id,
        token: res.body.capability,
        role: "player",
      });
      setMessage(`Joined game ${joinId}`);
    } else setMessage(res.body?.error?.message ?? "join failed");
  };

  const startGame = async () => {
    if (!identity) return;
    const res = await api("POST", `/v1/games/${identity.gameId}/start`, {
      token: identity.token,
      body: {},
    });
    setMessage(res.status === 200 ? "Game started" : (res.body?.error?.message ?? "start failed"));
  };

  const submit = async (action: Record<string, unknown>) => {
    if (!identity || !decision) return;
    const commandId = `${identity.playerId}:${decision.turn.id}:${crypto.randomUUID().slice(0, 8)}`;
    const res = await api("POST", `/v1/games/${identity.gameId}/commands`, {
      token: identity.token,
      body: { commandId, turnId: decision.turn.id, action },
    });
    setMessage(
      res.status === 200 ? `${action.type as string} ok` : (res.body?.error?.code ?? "rejected"),
    );
    await refresh();
  };

  const colorOf = (playerId?: string): string =>
    board?.players.find((p) => p.id === playerId)?.color ?? "#e0e0e0";

  const territory = (id: string) => board?.territories.find((t) => t.id === id);

  return (
    <div className="app">
      <h1>Streamsy Risk</h1>
      {board && (
        <div className="status">
          <span>Status: {board.game.status}</span>
          <span>Round: {board.game.round}</span>
          <span>Phase: {board.game.phase ?? "—"}</span>
          <span>
            Active:{" "}
            <b style={{ color: colorOf(board.game.activePlayerId) }}>
              {board.players.find((p) => p.id === board.game.activePlayerId)?.name ?? "—"}
            </b>
          </span>
          {board.game.winnerId && (
            <span className="winner">
              Winner: {board.players.find((p) => p.id === board.game.winnerId)?.name}
            </span>
          )}
        </div>
      )}

      <div className="board">
        {LAYOUT.map((row, i) => (
          <div className="row" key={i}>
            {row.map((id) => {
              const t = territory(id);
              return (
                <div
                  className="territory"
                  key={id}
                  style={{
                    background: colorOf(t?.ownerId),
                    cursor: decision ? "pointer" : "default",
                  }}
                  onClick={() =>
                    decision?.turn.phase === "reinforce" &&
                    submit({ type: "reinforce", territoryId: id, armies: 1 })
                  }
                  title={id}
                >
                  <div className="tname">{id}</div>
                  <div className="tarmies">{t?.armies ?? 0}</div>
                </div>
              );
            })}
          </div>
        ))}
      </div>

      {decision && (
        <div className="actions">
          <b>Your turn ({decision.turn.phase}):</b>
          {decision.legalActions.map((a: any, idx: number) => {
            if (a.type === "attack") {
              return a.choices.map((c: any) => (
                <button
                  key={`atk-${c.from}-${c.to}`}
                  onClick={() =>
                    submit({
                      type: "attack",
                      from: c.from,
                      to: c.to,
                      attackerDice: c.maxAttackerDice,
                    })
                  }
                >
                  attack {c.from}→{c.to}
                </button>
              ));
            }
            if (a.type === "end-turn") {
              return (
                <button key="end" onClick={() => submit({ type: "end-turn" })}>
                  end turn
                </button>
              );
            }
            if (a.type === "reinforce") {
              return (
                <span key="reinf" className="hint">
                  click a territory you own to reinforce ({a.maxArmies} left)
                </span>
              );
            }
            return <span key={idx} />;
          })}
        </div>
      )}

      <div className="controls">
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="name" />
        <input value={color} onChange={(e) => setColor(e.target.value)} type="color" />
        <button onClick={createGame}>New game</button>
        <input
          value={joinId}
          onChange={(e) => setJoinId(e.target.value)}
          placeholder="game id to join"
        />
        <button onClick={joinGame}>Join</button>
        {identity?.role === "host" && <button onClick={startGame}>Start</button>}
        {identity && (
          <button onClick={() => persist(null)} className="leave">
            Leave
          </button>
        )}
      </div>

      {identity && (
        <p className="you">
          You are <b>{identity.playerId}</b> in game <code>{identity.gameId}</code> ({identity.role}
          )
        </p>
      )}
      {message && <p className="message">{message}</p>}
    </div>
  );
}
