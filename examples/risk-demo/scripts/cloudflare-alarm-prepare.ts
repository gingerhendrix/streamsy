/* oxlint-disable effecttsgo/async-function -- This executable script is a bounded Promise-native Bun/Node adapter over the demo's public HTTP and application APIs. */
/* oxlint-disable effecttsgo/global-console, effecttsgo/global-fetch, effecttsgo/process-env -- This executable smoke preparer directly owns its HTTP, terminal, and environment boundary. */
/** Prepare a human-vs-human pending defence for DO alarm/restart verification. */
import { Schema } from "effect";

const baseUrl = new URL(process.env.BASE_URL ?? "http://127.0.0.1:8791").origin;

async function call(
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<any> {
  const headers = new Headers({ "content-type": "application/json" });
  if (options.token) headers.set("authorization", `Bearer ${options.token}`);
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
const JsonRecord = Schema.Record(Schema.String, Schema.Json);
const checkedRecord = Schema.decodeUnknownSync(JsonRecord);
const checkedString = Schema.decodeUnknownSync(Schema.String);
const gameId = checkedString(checkedRecord(created.game).id);
const hostId = checkedString(checkedRecord(created.player).id);
const tokens = new Map([[hostId, checkedString(created.capability)]]);
const joined = await call("POST", `/v1/games/${gameId}/players`, {
  body: { name: "Alarm Defender", color: "blue" },
});
tokens.set(checkedString(checkedRecord(joined.player).id), checkedString(joined.capability));
await call("POST", `/v1/games/${gameId}/start`, {
  token: tokens.get(hostId),
  body: { commandId: "alarm-smoke-start" },
});
const metadata = await call("GET", `/v1/games/${gameId}`);
const active = checkedString(metadata.activePlayerId);
let decision = await call("GET", `/v1/games/${gameId}/decision`, { token: tokens.get(active) });
const reinforce = decision.legalMoves.find(
  (action: { type: string }) => action.type === "reinforce",
);
if (!reinforce) throw new Error("no reinforce action");
await call("POST", `/v1/games/${gameId}/commands`, {
  token: tokens.get(active),
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
decision = await call("GET", `/v1/games/${gameId}/decision`, { token: tokens.get(active) });
const attack = decision.legalMoves.find(
  (action: { type: string }) => action.type === "declare-attack",
);
if (!attack) throw new Error("no declare-attack action");
const target = attack.choices[0];
const declared = await call("POST", `/v1/games/${gameId}/commands`, {
  token: tokens.get(active),
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
