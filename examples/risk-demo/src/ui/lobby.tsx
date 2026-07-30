/**
 * The `Hex Domination` lobby, set as a pre-deployment briefing page.
 *
 * Two panes under one brief: the muster roll answers *who is going to war*, the
 * terrain survey answers *where*. The muster roll always shows every seat —
 * filled rows carry the player's canonical colour swatch and role annotations,
 * open rows say plainly whether they are needed or optional — so the state of the
 * lobby is legible at a glance instead of implied by blank space.
 *
 * Commands are normal-sized rectangles in one row, in Field Manual weight order:
 * start (olive primary), open an agent seat, copy the invite — preceded, for a host
 * with a free seat, by the name the next agent seat will carry. Nothing here decides
 * game legality — starting is validated canonically like every other command.
 */

import { useState } from "react";

import type { ProjectedPlayer } from "../board/projection.ts";
import { RULES } from "../domain/map.ts";
import { LobbyTerrainPreview } from "./lobby-preview.tsx";
import { MAX_SEAT_NAME, playerRoleLabel, type Identity } from "./shared.tsx";

/** A seat opened for a user-supplied coding agent, with its pasteable instructions. */
export interface AgentSeat {
  playerId: string;
  name: string;
  instructions: string;
}

function seatAnnotations(props: {
  player: ProjectedPlayer;
  hostPlayerId?: string;
  selfPlayerId?: string;
}): string {
  const { player } = props;
  return [
    playerRoleLabel(props.hostPlayerId, player.id),
    player.controller === "external-agent" ? "agent" : player.controller === "bot" ? "bot" : null,
    player.id === props.selfPlayerId ? "you" : null,
  ]
    .filter((part): part is string => part !== null)
    .join(" · ");
}

export function Lobby(props: {
  players: ProjectedPlayer[];
  hostPlayerId?: string;
  identity: Identity | null;
  /** True when this browser holds no playable seat — a host that delegated its own. */
  spectating: boolean;
  name: string;
  busy: boolean;
  agentSeats: AgentSeat[];
  mapSeed?: string;
  onName(value: string): void;
  onJoin(): void;
  onStart(): void;
  onAddAgent(name: string): void;
  onCopy(): Promise<void>;
}) {
  const isHost = props.identity?.role === "host";
  const seatCount = RULES.maxPlayers;
  const seated = props.players.length;
  const [agentName, setAgentName] = useState("");
  // Only a seat the reader actually plays is annotated "you"; a host spectating its
  // own delegated seat is watching that seat, not holding it.
  const selfPlayerId = props.spectating ? undefined : props.identity?.playerId;

  return (
    <section className="lobby-current">
      <header className="lobby-brief">
        <span className="section-label">
          Lobby · {seated}/{seatCount} seats
        </span>
        <h2>{seated < RULES.minPlayers ? "Waiting for a challenger" : "Ready to deploy"}</h2>
        <p>
          {isHost
            ? "Share the link or open an agent seat, then start when everyone has arrived."
            : props.identity
              ? "The host will begin when the lobby is ready."
              : "Choose a name and claim a player seat."}
        </p>
      </header>

      <div className="lobby-columns">
        <div className="lobby-muster">
          <div className="panel-heading">
            <h2>Muster roll</h2>
            <span>
              {seated} of {seatCount} seats filled
            </span>
          </div>
          <ul className="muster-list">
            {Array.from({ length: seatCount }, (_, index) => {
              const player = props.players[index];
              if (!player) {
                return (
                  <li className="muster-row open" key={`open-seat-${index}`}>
                    <span className="muster-index">{String(index + 1).padStart(2, "0")}</span>
                    <span className="player-color open" aria-hidden="true" />
                    <div>
                      <b>Open seat</b>
                      <small>
                        {index < RULES.minPlayers ? "Needed to start" : "Optional reinforcement"}
                      </small>
                    </div>
                    <span className="muster-status open">Awaiting</span>
                  </li>
                );
              }
              return (
                <li className="muster-row" key={player.id}>
                  <span className="muster-index">{String(index + 1).padStart(2, "0")}</span>
                  <span className="player-color" style={{ background: player.color }} />
                  <div>
                    <b>{player.name}</b>
                    <small>
                      {seatAnnotations({
                        player,
                        hostPlayerId: props.hostPlayerId,
                        selfPlayerId,
                      })}
                    </small>
                  </div>
                  <span className="muster-status">Ready</span>
                </li>
              );
            })}
          </ul>

          {!props.identity && (
            <div className="muster-join">
              {/* Name only: the seat's colour is issued canonically on join, so
                  two players can never claim one colour or race for a swatch. */}
              <div className="player-fields">
                <label>
                  <span>Your name</span>
                  <input
                    value={props.name}
                    maxLength={24}
                    onChange={(event) => props.onName(event.target.value)}
                  />
                </label>
              </div>
              <button
                className="primary"
                onClick={props.onJoin}
                disabled={props.busy || !props.name.trim()}
              >
                {props.busy ? "Joining…" : "Join this game"}
              </button>
              <p className="muted">Your standard colour is issued when you take the seat.</p>
            </div>
          )}

          {isHost && seated < seatCount && (
            // Named before the seat is opened, because the name is fixed on join:
            // the seat's instructions, the muster roll, and the move feed all carry
            // it, and there is no rename command.
            <div className="player-fields compact">
              <label>
                <span>Agent name</span>
                <input
                  value={agentName}
                  maxLength={MAX_SEAT_NAME}
                  placeholder={`Agent ${seated + 1}`}
                  onChange={(event) => setAgentName(event.target.value)}
                />
              </label>
            </div>
          )}

          <div className="lobby-actions">
            {isHost && (
              <button
                className="primary"
                onClick={props.onStart}
                disabled={props.busy || seated < RULES.minPlayers}
              >
                {seated < RULES.minPlayers ? "Waiting for 2 players" : "Start game"}
              </button>
            )}
            {isHost && (
              <button
                onClick={() => {
                  props.onAddAgent(agentName);
                  setAgentName("");
                }}
                disabled={props.busy || seated >= seatCount}
              >
                Open an agent seat
              </button>
            )}
            <button onClick={() => void props.onCopy()}>Copy invite link</button>
          </div>

          {props.agentSeats.map((seat) => (
            <div className="agent-seat" key={seat.playerId}>
              <b>{seat.name} is seated.</b>
              <span>Paste this complete block into your coding-agent UI:</span>
              <textarea
                readOnly
                rows={14}
                value={seat.instructions}
                aria-label={`${seat.name} instructions`}
              />
              <button
                onClick={async () => {
                  await navigator.clipboard.writeText(seat.instructions);
                }}
              >
                Copy agent instructions
              </button>
            </div>
          ))}
        </div>

        <aside className="lobby-survey">
          <div className="panel-heading">
            <h2>Terrain survey</h2>
            <span>Advance copy</span>
          </div>
          <LobbyTerrainPreview
            key={`${props.mapSeed ?? "pending"}:${seated}`}
            seed={props.mapSeed}
            playerCount={seated}
          />
          <p className="survey-note">
            Drawn from this game's recorded map seed. The sheet is re-surveyed if the roster changes
            size; the canonical snapshot issued at the start of the game is final.
          </p>
        </aside>
      </div>
    </section>
  );
}
