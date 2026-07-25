/**
 * External-agent bootstrap helpers shared by the lobby and HTTP surface.
 *
 * The bearer capability is placed only in the URL fragment. Fragments are
 * resolved by the client and are never part of an HTTP request target.
 */

export interface AgentSeatIdentity {
  origin: string;
  gameId: string;
  playerId: string;
}

export function agentSeatPath(gameId: string, playerId: string): string {
  return `/agent-seat/${encodeURIComponent(gameId)}/${encodeURIComponent(playerId)}`;
}

/** Build the complete private URL returned once to the seat creator. */
export function privateAgentSeatUrl(identity: AgentSeatIdentity & { capability: string }): string {
  const url = new URL(agentSeatPath(identity.gameId, identity.playerId), identity.origin);
  url.hash = `token=${encodeURIComponent(identity.capability)}`;
  return url.toString();
}

/** Concise, harness-neutral protocol instructions; strategic choice stays external. */
export function agentSeatPrompt(privateSeatUrl = "<PRIVATE_SEAT_URL>"): string {
  return `You control one seat in Streamsy Risk through its published HTTP API.

Private seat URL: ${privateSeatUrl}

Security:
- Treat the complete seat URL and bearer capability as secrets. Never print, summarize, persist, commit, log, or screenshot either one.
- Parse the URL fragment locally. Fetch the URL without its fragment, and send the token only as Authorization: Bearer <token>. Never put it in a query string or path.
- Do not inspect or import the game repository. Use only the seat document, OpenAPI, and HTTP resources on the declared origin.

Control loop:
1. Fetch the fragment-free seat document, then its declared /openapi.json.
2. Follow the authenticated player-turn stream with its returned cursor and a bounded long poll using ?offset=<returned-cursor>&wait=<milliseconds>. The parameter is offset, not cursor.
3. Treat every wake, timeout, stale response, or retry as a hint: fetch a fresh authenticated /decision before acting.
4. Stop only when public game metadata has canonical status finished. If legalActions is empty, retain the newest cursor and wait again.
5. Choose at most one action allowed by the fresh legalActions. Fetch /board when broader map context helps. Out-of-turn roll-defense and mandatory occupation are actionable.
6. Submit exactly the fresh turn.id with a new stable commandId. Preserve a byte-equivalent command ID and payload across transport retries; accepted and a matching duplicate are success.
7. On any stale-turn, ownership, already-resolved, timeout-race, or other conflict, discard the old decision and fetch a fresh one. After every accepted command, return to the wake/fresh-decision loop.

Use bounded waits and caller-supplied duration, command, and spend limits. Keep only the latest cursor and any in-flight retry payload, without the bearer token.`;
}

/** Render the public document fetched after the harness strips its URL fragment. */
export function agentSeatBootstrapDocument(identity: AgentSeatIdentity): string {
  const origin = new URL(identity.origin).origin;
  const gamePath = `/v1/games/${encodeURIComponent(identity.gameId)}`;

  return `Streamsy Risk external-agent seat bootstrap

This document is NON-SECRET. The complete seat URL you were given is SECRET.
URL fragments are never sent to this server. Parse #token=<capability> locally,
strip the fragment before every fetch, and use the capability only as:
Authorization: Bearer <capability>

Never place the capability in a query string or path. Never print, persist, log,
commit, summarize, or screenshot the complete private seat URL or bearer header.

API origin: ${origin}
OpenAPI: ${origin}/openapi.json
Game ID: ${identity.gameId}
Player ID: ${identity.playerId}

Resources:
- Metadata: GET ${gamePath}
- Decision: GET ${gamePath}/decision
- Commands: POST ${gamePath}/commands
- Board: GET ${gamePath}/board
- Player turns: GET ${gamePath}/players/me/turns
  Long poll: GET ${gamePath}/players/me/turns?offset=<returned-cursor>&wait=30000
  Use the response cursor as the next offset. Both query parameters are optional;
  wait is a bounded duration in milliseconds. Do not use cursor=.

Harness-neutral prompt:

${agentSeatPrompt()}
`;
}
