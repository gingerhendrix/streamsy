/** Low-friction, single-session HTTP contract for an external Risk player. */

export interface AgentPlayUrls {
  origin: string;
  gameId: string;
  playerId: string;
  token: string;
}

export function agentStatePath(gameId: string, token: string): string {
  return `/v1/games/${encodeURIComponent(gameId)}/agent/${encodeURIComponent(token)}/state`;
}

export function agentWaitPath(gameId: string, token: string): string {
  return `/v1/games/${encodeURIComponent(gameId)}/agent/${encodeURIComponent(token)}/wait?wait=30000`;
}

/** Complete copy-paste instructions. The capability is intentionally in agent context. */
export function agentPlayInstructions(input: AgentPlayUrls): string {
  const origin = new URL(input.origin).origin;
  const game = `/v1/games/${encodeURIComponent(input.gameId)}`;
  const stateUrl = `${origin}${agentStatePath(input.gameId, input.token)}`;
  const waitUrl = `${origin}${agentWaitPath(input.gameId, input.token)}`;
  const commandsUrl = `${origin}${game}/commands`;

  return `You are playing one seat in Streamsy Hex Domination. Play the whole game in this session using plain fetch or curl. Do not inspect the game repository and do not ask the human to choose moves for you.

Your seat:
- Player ID: ${input.playerId}
- Token: ${input.token}
- Wait URL: ${waitUrl}
- State URL: ${stateUrl}
- Commands endpoint: ${commandsUrl}

The state URL is the one source you need: it contains every territory's id, name, neighbours, owner and armies, current turn/interrupt information, and your current legalMoves. The wait and state URLs already contain your seat token. For commands, send Authorization: Bearer ${input.token} and Content-Type: application/json.

Control loop:
1. GET the wait URL. A wake or timeout is only a hint that something may have changed.
2. GET the state URL immediately before choosing a move. Never act from an older state.
3. If status is finished, report the winner and stop. If legalMoves is empty, return to step 1.
4. Choose one move exactly allowed by legalMoves, including its ids and numeric bounds. A reinforce move is the whole reinforcement turn: submit one placements array with distinct owned territory ids whose armies sum exactly to maxArmies. Strategy is yours, but the server is authoritative.
5. POST {"commandId":"<stable unique id>","turnId":"<fresh turn.id>","action":<chosen move>} to the commands endpoint. Legal move types are reinforce, declare-attack, occupy-territory, fortify, and end-turn. Defence dice are rolled automatically for agent seats; only human players receive the roll-defense prompt.
6. A successful capture creates a mandatory occupy-territory move; complete it before anything else.
7. Generate commandId once per intended command. On a transport failure, retry the byte-identical body with the same commandId. Treat accepted and duplicate as success. Never reuse that commandId for different input.
8. On any rejection or conflict, read its message, discard the old choice, fetch the state URL again, and choose from the new legalMoves. Do not mutate and resend a stale command.
9. After every accepted/duplicate command, return to step 1. One turn can require several consecutive combat or manoeuvre moves, but reinforcement is always one command.

Start now and continue until the game is finished.`;
}
