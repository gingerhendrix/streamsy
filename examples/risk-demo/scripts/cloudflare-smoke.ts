/* oxlint-disable effecttsgo/async-function -- This executable script is a bounded Promise-native Bun/Node adapter over the demo's public HTTP and application APIs. */
/* oxlint-disable effecttsgo/extends-native-error, effecttsgo/global-console, effecttsgo/global-date, effecttsgo/global-fetch, effecttsgo/process-env -- This standalone smoke executable uses native errors for terminal failure and directly owns Web requests, timestamps, output, and environment configuration. */
/**
 * Runtime-neutral smoke for a running Cloudflare Worker (local or deployed).
 * It prints no capabilities and uses only disposable games.
 */

import { createActionsReader, type ActionsReader } from "../src/application/actions-stream.ts";

const baseUrl = new URL(process.env.BASE_URL ?? "http://127.0.0.1:8791").origin;

class SmokeError extends Error {}
function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new SmokeError(message);
}

async function api(
  method: string,
  path: string,
  options: { token?: string; body?: unknown; accept?: string } = {},
): Promise<{ status: number; body: any }> {
  // The actions resource streams unless a caller negotiates the JSON reading.
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: options.accept ?? "application/json",
  };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const body = await response.json().catch(() => null);
  return { status: response.status, body };
}

/**
 * Open the actions resource as what it is: a Server-Sent Events stream, over a
 * connection this caller can actually abort. The reader's `close` aborts it and
 * waits for the read to settle, so a held connection is dropped at the edge
 * rather than left running behind this smoke.
 */
async function openActions(
  gameId: string,
  token: string,
  offset?: string,
): Promise<{ response: Response; reader: ActionsReader }> {
  const query = offset ? `?offset=${encodeURIComponent(offset)}` : "";
  const connection = new AbortController();
  const response = await fetch(`${baseUrl}/v1/games/${gameId}/players/me/actions${query}`, {
    headers: { accept: "text/event-stream", authorization: `Bearer ${token}` },
    signal: connection.signal,
  });
  return { response, reader: createActionsReader(response, connection) };
}

async function main(): Promise<void> {
  const health = await api("GET", "/healthz");
  assert(health.status === 200 && health.body.ok === true, "health check failed");

  const created = await api("POST", "/v1/games", {
    body: { name: "Cloud Host", color: "red" },
  });
  assert(created.status === 201, `create returned ${created.status}`);
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This bounded smoke executable immediately verifies the local demo response fields before using them and exits on contract mismatch.
  const gameId = created.body.game.id as string;
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This bounded smoke executable immediately verifies the local demo response fields before using them and exits on contract mismatch.
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
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This bounded smoke executable immediately verifies the local demo response fields before using them and exits on contract mismatch.
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
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This bounded smoke executable immediately verifies the local demo response fields before using them and exits on contract mismatch.
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

  // The actions stream is served *inside* the per-game Durable Object and must
  // survive the edge intact: correct SSE framing, an unbuffered first batch, and
  // a connection genuinely held open rather than cut short by a proxy. None of
  // that is provable from a local harness.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This bounded smoke executable immediately verifies the local demo response fields before using them and exits on contract mismatch.
  const seatToken = seat.body.seat.token as string;
  const opened = await openActions(agentGameId, seatToken);
  assert(opened.response.status === 200, `actions stream returned ${opened.response.status}`);
  assert(
    (opened.response.headers.get("content-type") ?? "").includes("text/event-stream"),
    `actions stream is ${opened.response.headers.get("content-type")}`,
  );
  assert(
    opened.response.headers.get("cache-control") === "no-store",
    "the actions stream is cacheable",
  );
  const opening = await opened.reader.next(10_000);
  await opened.reader.close();
  assert(opening !== null, "the edge delivered no opening batch");
  assert(typeof opening.nextOffset === "string", "no control frame cursor across the edge");

  // Reconnecting from that cursor restates it, delivers nothing twice, and then
  // holds — the streaming equivalent of the bounded wait this replaced.
  const resumed = await openActions(agentGameId, seatToken, opening.nextOffset);
  const caughtUp = await resumed.reader.next(10_000);
  assert(
    caughtUp !== null && caughtUp.messages.length === 0,
    "a resumed actions stream replayed or lost messages across the edge",
  );
  assert(caughtUp.nextOffset === opening.nextOffset, "the resumed cursor moved");
  const startedAt = Date.now();
  const held = await resumed.reader.next(2_000);
  assert(held === null, "the edge cut a held actions stream short");
  assert(Date.now() - startedAt >= 1_500, "the held connection ended early");
  // Aborting a held connection settles the reader rather than leaving a read
  // running against the edge.
  await resumed.reader.close();
  assert(
    (await resumed.reader.next(250)) === null,
    "a closed actions reader kept producing batches",
  );

  // The long poll it replaced is refused rather than quietly reinterpreted.
  const waited = await api("GET", `/v1/games/${agentGameId}/players/me/actions?wait=2000`, {
    token: seatToken,
  });
  assert(waited.status === 400, `the removed long poll returned ${waited.status}`);

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
        "actions stream SSE framing, resume, and hold across the edge",
        "SPA fallback",
      ],
    }),
  );
}

main().catch((error) => {
  console.error(error instanceof SmokeError ? error.message : error);
  process.exit(1);
});
