/** Prepare a human-vs-human pending defence for DO alarm/restart verification. */

const baseUrl = new URL(process.env.BASE_URL ?? "http://127.0.0.1:8791").origin;

async function call(
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<any> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const body = await response.json();
  if (!response.ok) throw new Error(`${method} ${path} returned ${response.status}`);
  return body;
}

const created = await call("POST", "/v1/games", {
  body: { name: "Alarm Host", color: "red", mapSeed: "cloudflare-alarm-restart" },
});
const gameId = created.game.id as string;
const hostId = created.player.id as string;
const tokens: Record<string, string> = { [hostId]: created.capability };
const joined = await call("POST", `/v1/games/${gameId}/players`, {
  body: { name: "Alarm Defender", color: "blue" },
});
tokens[joined.player.id] = joined.capability;
await call("POST", `/v1/games/${gameId}/start`, {
  token: tokens[hostId],
  body: { commandId: "alarm-smoke-start" },
});
const metadata = await call("GET", `/v1/games/${gameId}`);
const active = metadata.activePlayerId as string;
let decision = await call("GET", `/v1/games/${gameId}/decision`, { token: tokens[active] });
const reinforce = decision.legalMoves.find(
  (action: { type: string }) => action.type === "reinforce",
);
if (!reinforce) throw new Error("no reinforce action");
await call("POST", `/v1/games/${gameId}/commands`, {
  token: tokens[active],
  body: {
    commandId: "alarm-smoke-reinforce",
    turnId: decision.turn.id,
    action: {
      type: "reinforce",
      territoryId: reinforce.territoryIds[0],
      armies: reinforce.pool,
    },
  },
});
decision = await call("GET", `/v1/games/${gameId}/decision`, { token: tokens[active] });
const attack = decision.legalMoves.find(
  (action: { type: string }) => action.type === "declare-attack",
);
if (!attack) throw new Error("no declare-attack action");
const target = attack.choices[0];
const declared = await call("POST", `/v1/games/${gameId}/commands`, {
  token: tokens[active],
  body: {
    commandId: "alarm-smoke-declare",
    turnId: decision.turn.id,
    action: {
      type: "declare-attack",
      from: target.from,
      to: target.to,
      attackerDice: target.maxAttackerDice,
    },
  },
});
const event = declared.events.find(
  (candidate: { type: string }) => candidate.type === "AttackDeclared",
);
if (!event) throw new Error("attack was not declared");

// Intentionally no tokens: this output is safe to retain as verification evidence.
console.log(
  JSON.stringify({
    gameId,
    attackId: event.attackId,
    defenseDeadlineAt: event.defenseDeadlineAt,
  }),
);
