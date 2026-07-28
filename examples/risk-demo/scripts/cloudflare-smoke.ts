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
    body: { name: "Cloud Host", color: "red" },
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
  const reinforce = decision.body.legalMoves.find(
    (action: { type: string }) => action.type === "reinforce",
  );
  assert(reinforce, "no reinforce action");
  const command = {
    commandId: "cloud-smoke-reinforce",
    turnId: decision.body.turn.id,
    action: {
      type: "reinforce",
      placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
    },
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
    board.body.sourceThroughOffset === accepted.body.eventOffset,
    "board watermark did not reach command",
  );
  const publicBoard = await fetch(
    `${baseUrl}/streams/games/${gameId}/projections/board/${board.body.generation}`,
  );
  assert(publicBoard.status === 200, `active spectator stream returned ${publicBoard.status}`);
  const canonical = await fetch(`${baseUrl}/streams/games/${gameId}/events`);
  assert(canonical.status === 404, "canonical event stream became public");

  const agentGame = await api("POST", "/v1/games", {
    body: { name: "Agent Host" },
  });
  assert(agentGame.status === 201, "agent game create failed");
  const agentGameId = agentGame.body.game.id as string;
  assert(agentGameId !== gameId, "two creates returned the same game id");
  const seat = await api("POST", `/v1/games/${agentGameId}/agent-seats`, {
    token: agentGame.body.capability,
    body: { playerId: agentGame.body.player.id },
  });
  assert(seat.status === 201, "agent seat delegation failed");

  // Isolation is stronger on Cloudflare than in a single-process host, and the
  // status code says which mechanism enforced it. Each game is its own Durable
  // Object with its own capability table, so a token minted for another game is
  // not merely out of scope — it does not exist here at all, and authentication
  // fails (401) before any game-scoping check could answer 403. A single-process
  // deployment shares one capability store and answers 403 WRONG_GAME. Both are
  // a refusal; this smoke runs against the edge, so it asserts the edge's.
  const crossGame = await api("GET", `/v1/games/${gameId}/decision`, {
    token: seat.body.seat.token,
  });
  assert(
    crossGame.status === 401,
    `an agent capability crossed game isolation (expected 401 from an isolated Durable Object, got ${crossGame.status})`,
  );
  assert(
    crossGame.body?.error?.code === "UNAUTHORIZED",
    `cross-game read reported ${crossGame.body?.error?.code} rather than UNAUTHORIZED`,
  );

  // A host may hand over its own seat and no one else's.
  const foreignDelegation = await api("POST", `/v1/games/${agentGameId}/agent-seats`, {
    token: agentGame.body.capability,
    body: { playerId: "p_not_the_host" },
  });
  assert(
    foreignDelegation.status === 403,
    `foreign seat delegation returned ${foreignDelegation.status}`,
  );

  // The actions stream long-polls *inside* the per-game Durable Object. A bounded
  // wait that returns empty and up-to-date — without the edge cutting it short —
  // is the property no local harness can prove.
  const seatToken = seat.body.seat.token as string;
  const opening = await api("GET", `/v1/games/${agentGameId}/players/me/actions`, {
    token: seatToken,
  });
  assert(opening.status === 200, `actions read returned ${opening.status}`);
  const startedAt = Date.now();
  const held = await api(
    `GET`,
    `/v1/games/${agentGameId}/players/me/actions?offset=${opening.body.nextOffset}&wait=2000`,
    { token: seatToken },
  );
  const heldMs = Date.now() - startedAt;
  assert(held.status === 200, `long poll returned ${held.status}`);
  assert(held.body.messages.length === 0, "long poll on an unstarted game produced messages");
  assert(held.body.nextOffset === opening.body.nextOffset, "long poll moved the cursor");
  assert(heldMs >= 1500, `long poll returned after ${heldMs}ms instead of holding`);

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
        "two-game capability isolation (401 from an isolated Durable Object)",
        "agent seat authority (own seat only)",
        "actions stream bounded long poll",
        "SPA fallback",
      ],
    }),
  );
}

main().catch((error) => {
  console.error(error instanceof SmokeError ? error.message : error);
  process.exit(1);
});
