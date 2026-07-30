/**
 * The `Hex Domination` lobby, set as a pre-deployment briefing page.
 *
 * Two panes under one brief: the muster roll answers *who is going to war*, the
 * terrain survey answers *where*. The muster roll always shows every seat —
 * filled rows carry the player's canonical colour swatch and role annotations,
 * open rows say plainly whether they are needed or optional — so the state of the
 * lobby is legible at a glance instead of implied by blank space.
 *
 * Every command here is role-derived rather than mode-derived, because a browser's
 * role is a fact about the roster and its capability, not a screen it navigated to:
 * share is for everyone; join for a visitor holding no seat; rename for the seat
 * you hold, and for agent seats if you opened them; leave for a seat a person is
 * actually playing; opening an agent seat and starting for the creator. Nothing
 * here decides game legality — every command is validated canonically.
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

/**
 * The renameable-by-me test.
 *
 * Your own seat, and — for the creator — any agent seat, because the creator is
 * the only party that can open one. Deliberately not another person's seat: the
 * server refuses that for the same reason it refuses delegating one.
 */
export function canRenameSeat(props: {
  player: ProjectedPlayer;
  identity: Identity | null;
  isHost: boolean;
}): boolean {
  if (!props.identity) return false;
  if (props.player.id === props.identity.playerId) return true;
  return props.isHost && props.player.controller === "external-agent";
}

function SeatName(props: {
  player: ProjectedPlayer;
  editing: boolean;
  editable: boolean;
  busy: boolean;
  draft: string;
  onDraft(value: string): void;
  onEdit(): void;
  onCancel(): void;
  onSave(): void;
}) {
  if (!props.editable) return <b>{props.player.name}</b>;
  if (!props.editing) {
    return (
      <b>
        {props.player.name}{" "}
        <button
          className="link-command"
          onClick={props.onEdit}
          aria-label={`Rename ${props.player.name}`}
        >
          Rename
        </button>
      </b>
    );
  }
  return (
    <form
      className="seat-rename"
      onSubmit={(event) => {
        event.preventDefault();
        props.onSave();
      }}
    >
      <input
        value={props.draft}
        maxLength={MAX_SEAT_NAME}
        aria-label={`Name for ${props.player.name}`}
        onChange={(event) => props.onDraft(event.target.value)}
      />
      <button type="submit" className="primary" disabled={props.busy || !props.draft.trim()}>
        Save
      </button>
      <button type="button" onClick={props.onCancel}>
        Cancel
      </button>
    </form>
  );
}

export function Lobby(props: {
  players: ProjectedPlayer[];
  hostPlayerId?: string;
  identity: Identity | null;
  /** True when this browser holds no playable seat — a host that delegated or left its own. */
  spectating: boolean;
  name: string;
  busy: boolean;
  agentSeats: AgentSeat[];
  mapSeed?: string;
  onName(value: string): void;
  onJoin(): void;
  onStart(): void;
  onAddAgent(name: string): void;
  onRename(playerId: string, name: string): void;
  onLeave(): void;
  onCopy(): Promise<void>;
}) {
  const isHost = props.identity?.role === "host";
  const seatCount = RULES.maxPlayers;
  const seated = props.players.length;
  const [agentName, setAgentName] = useState("");
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [draftName, setDraftName] = useState("");
  const [confirmingLeave, setConfirmingLeave] = useState(false);
  // Only a seat the reader actually plays is annotated "you"; a host spectating its
  // own delegated seat is watching that seat, not holding it.
  const selfPlayerId = props.spectating ? undefined : props.identity?.playerId;
  // Leaving is giving up a seat, so it needs one: a visitor has nothing to leave,
  // and neither does a creator whose seat is now an agent's or already given up.
  const ownSeat = props.identity
    ? props.players.find((player) => player.id === props.identity!.playerId)
    : undefined;
  const canLeave = ownSeat !== undefined && ownSeat.controller === "human";

  return (
    <section className="lobby-current">
      <header className="lobby-brief">
        <span className="section-label">
          Lobby · {seated}/{seatCount} seats
        </span>
        <h2>{seated < RULES.minPlayers ? "Waiting for a challenger" : "Ready to deploy"}</h2>
        <p>
          {isHost
            ? ownSeat
              ? "Name your seat, share the link or invite an agent, then start when everyone has arrived."
              : "You are watching this one. Invite agents or share the link, then start when the roll is ready."
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
                    <SeatName
                      player={player}
                      editable={canRenameSeat({ player, identity: props.identity, isHost })}
                      editing={renamingId === player.id}
                      busy={props.busy}
                      draft={draftName}
                      onDraft={setDraftName}
                      onEdit={() => {
                        setRenamingId(player.id);
                        setDraftName(player.name);
                      }}
                      onCancel={() => setRenamingId(null)}
                      onSave={() => {
                        props.onRename(player.id, draftName);
                        setRenamingId(null);
                      }}
                    />
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
                    maxLength={MAX_SEAT_NAME}
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
            // Named before the seat is opened, because the seat's instructions
            // carry the name; the host can still rename it on the roll afterwards.
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
                Invite an agent
              </button>
            )}
            <button onClick={() => void props.onCopy()}>Copy invite link</button>
            {canLeave && !confirmingLeave && (
              <button onClick={() => setConfirmingLeave(true)}>Leave game</button>
            )}
          </div>

          {canLeave && confirmingLeave && (
            // Leaving is canonical and cannot be undone from here — a returning
            // visitor joins as a new seat — so it is confirmed, and the confirmation
            // says which of the two outcomes applies to this browser.
            <div className="leave-confirm" role="alertdialog" aria-label="Confirm leaving">
              <b>Give up your seat?</b>
              <p>
                {isHost
                  ? "You keep hosting this game and will watch it from here."
                  : "Your seat is released and you go back to the home page."}
              </p>
              <div className="lobby-actions">
                <button className="primary" onClick={props.onLeave} disabled={props.busy}>
                  Yes, leave
                </button>
                <button onClick={() => setConfirmingLeave(false)}>Stay</button>
              </div>
            </div>
          )}

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
