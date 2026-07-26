/**
 * Runtime-neutral smoke for a running Cloudflare Worker (local or deployed).
 * It prints no capabilities and uses only disposable games.
 */

const baseUrl = new URL(process.env.BASE_URL ?? "http://127.0.0.1:8791").origin;

class SmokeError extends Error {}
function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new SmokeError(message);
}

async function api(
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const body = await response.json().catch(() => null);
  return { status: response.status, body };
}

async function main(): Promise<void> {
  const health = await api("GET", "/healthz");
  assert(health.status === 200 && health.body.ok === true, "health check failed");

  const created = await api("POST", "/v1/games", {
    body: { ruleset: "risk-demo-v1", name: "Cloud Host", color: "red" },
  });
  assert(created.status === 201, `create returned ${created.status}`);
  const gameId = created.body.game.id as string;
  const hostId = created.body.player.id as string;
  const tokens: Record<string, string> = { [hostId]: created.body.capability };

  const joined = await api("POST", `/v1/games/${gameId}/players`, {
    body: { name: "Cloud Guest", color: "blue" },
  });
  assert(joined.status === 201, `join returned ${joined.status}`);
  tokens[joined.body.player.id] = joined.body.capability;

  const started = await api("POST", `/v1/games/${gameId}/start`, {
    token: tokens[hostId],
    body: { commandId: "cloud-smoke-start" },
  });
  assert(started.status === 200, `start returned ${started.status}`);

  const metadata = await api("GET", `/v1/games/${gameId}`);
  const active = metadata.body.activePlayerId as string;
  const decision = await api("GET", `/v1/games/${gameId}/decision`, {
    token: tokens[active],
  });
  const reinforce = decision.body.legalActions.find(
    (action: { type: string }) => action.type === "reinforce",
  );
  assert(reinforce, "no reinforce action");
  const command = {
    commandId: "cloud-smoke-reinforce",
    turnId: decision.body.turn.id,
    action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 1 },
  };
  const accepted = await api("POST", `/v1/games/${gameId}/commands`, {
    token: tokens[active],
    body: command,
  });
  assert(accepted.body.status === "accepted", "command was not accepted");
  const duplicate = await api("POST", `/v1/games/${gameId}/commands`, {
    token: tokens[active],
    body: command,
  });
  assert(duplicate.body.status === "duplicate", "command retry was not deduplicated");

  const board = await api("GET", `/v1/games/${gameId}/board`);
  assert(
    board.body.sourceThroughOffset === accepted.body.sourceOffset,
    "board watermark did not reach command",
  );
  const publicBoard = await fetch(
    `${baseUrl}/streams/games/${gameId}/projections/board/${board.body.generation}`,
  );
  assert(publicBoard.status === 200, `active spectator stream returned ${publicBoard.status}`);
  const canonical = await fetch(`${baseUrl}/streams/games/${gameId}/events`);
  assert(canonical.status === 404, "canonical event stream became public");

  const agentGame = await api("POST", "/v1/games", {
    body: { name: "Agent Host", color: "green", controller: "agent" },
  });
  assert(agentGame.status === 201, "agent game create failed");
  const agentGameId = agentGame.body.game.id as string;
  assert(agentGameId !== gameId, "two creates returned the same game id");
  const state = await api(
    "GET",
    `/v1/games/${agentGameId}/agent/${agentGame.body.capability}/state`,
  );
  assert(state.status === 200 && state.body.gameId === agentGameId, "agent state route failed");
  const wait = await api(
    "GET",
    `/v1/games/${agentGameId}/agent/${agentGame.body.capability}/wait?wait=0`,
  );
  assert(wait.status === 200, "agent wait route failed");
  const crossGame = await api(
    "GET",
    `/v1/games/${gameId}/agent/${agentGame.body.capability}/state`,
  );
  assert(crossGame.status === 401, "a capability crossed game DO isolation");

  const spa = await fetch(`${baseUrl}/games/${gameId}`);
  assert(spa.status === 200 && (await spa.text()).includes('id="root"'), "SPA fallback failed");

  console.log(
    JSON.stringify({
      ok: true,
      baseUrl,
      games: [gameId, agentGameId],
      checks: [
        "create/join/start/command/duplicate",
        "board/spectator restriction",
        "two-game capability isolation",
        "agent state/wait",
        "SPA fallback",
      ],
    }),
  );
}

main().catch((error) => {
  console.error(error instanceof SmokeError ? error.message : error);
  process.exit(1);
});
