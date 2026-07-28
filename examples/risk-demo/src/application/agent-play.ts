/** Single-source seat descriptor and pasteable instructions for an external agent. */
import type { AgentSeatDescriptor } from "./api.ts";

export interface AgentPlayUrls {
  origin: string;
  gameId: string;
  playerId: string;
  name?: string;
  color?: string;
  token: string;
}

export function agentSeatDescriptor(input: AgentPlayUrls): AgentSeatDescriptor {
  const origin = new URL(input.origin).origin;
  const game = `/v1/games/${encodeURIComponent(input.gameId)}`;
  return {
    origin,
    gameId: input.gameId,
    playerId: input.playerId,
    name: input.name ?? input.playerId,
    color: input.color ?? "",
    token: input.token,
    urls: {
      map: `${game}/map`,
      actions: `${game}/players/me/actions`,
      decision: `${game}/decision`,
      commands: `${game}/commands`,
    },
  };
}

export function agentPlayInstructions(input: AgentPlayUrls): string {
  const seat = agentSeatDescriptor(input);
  const absolute = (path: string) => `${seat.origin}${path}`;
  return `You are playing one seat in Streamsy Hex Domination. Play the whole game using HTTP. Do not create, join, or start games.

Your seat:
- Game ID: ${seat.gameId}
- Player ID: ${seat.playerId}
- Token: ${seat.token}
- Map: ${absolute(seat.urls.map)}
- Actions: ${absolute(seat.urls.actions)}
- Decision recovery: ${absolute(seat.urls.decision)}
- Commands: ${absolute(seat.urls.commands)}

Send Authorization: Bearer ${seat.token} on actions, decision, and commands requests. Send Content-Type: application/json on commands. The map is immutable and may be fetched once.

Control loop:
1. GET the actions endpoint with ?offset=<last nextOffset>&wait=30000. Omit offset on the first read.
2. Save nextOffset. If messages is empty, repeat. If the newest message is GameOver, report the winner and stop.
3. Act only on the newest ActionRequired. It is self-sufficient: turn, legalMoves, board ownership/armies, and canonical events since your previous message.
4. Choose exactly one action allowed by legalMoves and follow that move's submit template. Reinforcement placements must use distinct listed territoryIds and sum exactly to pool.
5. POST {"commandId":"<stable unique id>","turnId":"<message turn.id>","action":<chosen action>} to commands.
6. Treat accepted and duplicate as success. Retry a transport failure byte-identically with the same commandId. Never reuse a commandId for different input.
7. On any rejection, return to the actions stream and act on its newest message. Use decision only for bootstrap/recovery if the stream and rejection appear inconsistent.

The ack is a receipt, not the outcome: {"status","commandId","turnId","eventOffset"}. What your move actually did arrives on the actions stream.

If you persist your cursor, only advance it past an ActionRequired once that message's command has been accepted or duplicated. An ActionRequired is never re-announced, so a cursor saved past an unanswered one waits forever. Keep the exact request body until then so a restart can replay it unchanged.

The map does not exist until the game starts; a 409 there means "not yet", so keep polling the actions stream.

Canonical event and message type values use PascalCase. Command action type values use kebab-case. Agent defence is server-resolved; agent seats never submit roll-defense.

Start now and continue until GameOver.`;
}
