/**
 * Pure presentation helpers for the `risk-demo-v2` playing surface.
 *
 * Everything a player reads — the reinforcement equation, the current-turn ledger,
 * the defence countdown, the dice pairing — is derived here so it can be tested
 * without a DOM, and so the components stay layout only.
 *
 * Two rules from the design spec are enforced by construction rather than by
 * convention:
 *
 *  - **No architecture in the product surface.** Nothing here formats an offset, a
 *    watermark, or a generation. The compact sync pill is the only place those
 *    exist, and it is not built from these helpers.
 *  - **The ledger reads the `turn` row.** `moves` is bounded at 40 rows and a v2
 *    turn burns roughly three events per throw, so paging it for "what happened
 *    this turn" silently loses the start of a busy turn. The projection's `turn`
 *    row is the derived summary that cannot (design spec §7.1).
 */

import type { ProjectedDiceV2, ProjectedMoveV2, ProjectedTurnV2 } from "../board/projection-v2.ts";
import type { ReinforcementState } from "../domain/aggregate-v2.ts";
import type { DefenseResolutionSource } from "../domain/events-v2.ts";
import type { Terrain } from "../domain/map-v2.ts";

/** Names the presentation layer needs but the projection rows only reference by id. */
export interface NameLookup {
  territory(id: string): string;
  player(id: string | undefined): string;
  continent(id: string): string;
}

// ---------------------------------------------------------------------------
// Reinforcement accounting
// ---------------------------------------------------------------------------

/** `"6 total = 4 territory + 2 Northreach"` — the whole pool, explained. */
export function reinforcementEquation(
  reinforcement: ReinforcementState,
  continentName: (id: string) => string,
): string {
  const parts = [`${reinforcement.base} territory`].concat(
    reinforcement.continents.map((bonus) => `${bonus.bonus} ${continentName(bonus.continentId)}`),
  );
  return `${reinforcement.total} total = ${parts.join(" + ")}`;
}

/** `"4 placed · 2 remaining"`. */
export function reinforcementProgress(placed: number, remaining: number): string {
  return `${placed} placed · ${remaining} remaining`;
}

// ---------------------------------------------------------------------------
// Defence countdown
// ---------------------------------------------------------------------------

/**
 * Whole seconds left on the canonical deadline, never negative. The deadline is a
 * recorded event value, so this is a *display* of canonical state — the client
 * never decides when the window closes.
 */
export function countdownSeconds(deadlineAt: number, now: number): number {
  return Math.max(0, Math.ceil((deadlineAt - now) / 1000));
}

export function countdownLabel(deadlineAt: number, now: number): string {
  const seconds = countdownSeconds(deadlineAt, now);
  return seconds === 0 ? "time’s up" : `${seconds}s`;
}

/** Fraction of the window still open, clamped to 0..1, for a progress ring. */
export function countdownFraction(deadlineAt: number, now: number, windowMs: number): number {
  if (windowMs <= 0) return 0;
  return Math.min(1, Math.max(0, (deadlineAt - now) / windowMs));
}

// ---------------------------------------------------------------------------
// Dice
// ---------------------------------------------------------------------------

export interface DicePair {
  attacker: number | null;
  defender: number | null;
  /** Who lost an army on this comparison; `null` where one side has no die. */
  loser: "attacker" | "defender" | null;
}

/**
 * Sort both rows descending and pair them the way the rules compare them: highest
 * against highest, ties to the defender. Unpaired dice are shown but decide nothing.
 */
export function dicePairs(
  attackerRolls: readonly number[],
  defenderRolls: readonly number[],
): DicePair[] {
  const attacker = attackerRolls.toSorted((a, b) => b - a);
  const defender = defenderRolls.toSorted((a, b) => b - a);
  const pairs: DicePair[] = [];
  for (let index = 0; index < Math.max(attacker.length, defender.length); index += 1) {
    const a = attacker[index] ?? null;
    const d = defender[index] ?? null;
    pairs.push({
      attacker: a,
      defender: d,
      loser: a === null || d === null ? null : a > d ? "defender" : "attacker",
    });
  }
  return pairs;
}

/** The two ways a roll happens without anyone clicking; the only copy for them. */
const AUTO_ROLL_LABELS: Record<Exclude<DefenseResolutionSource, "human">, string> = {
  "agent-auto": "Agent auto-rolled",
  timeout: "Auto-rolled after timeout",
};

/** How the defence roll was authorised — never implying a human clicked when none did. */
export function resolutionLabel(source: DefenseResolutionSource, defenderName: string): string {
  return source === "human" ? `Rolled by ${defenderName}` : AUTO_ROLL_LABELS[source];
}

/**
 * The same fact as a sentence about a country, for the history feed.
 *
 * Design spec §8.5.6: a defence the human never touched must not read as one they
 * did. The combat card has always branched on this; history and the ledger now
 * say it too, so a player scrolling back cannot mistake a lapsed window for a roll.
 */
export function defenseAttribution(
  source: DefenseResolutionSource,
  defenderName: string,
  territoryName: string,
): string {
  switch (source) {
    case "human":
      return `${defenderName} defended ${territoryName}`;
    case "agent-auto":
      return `${defenderName} auto-rolled the defence of ${territoryName}`;
    case "timeout":
      return `${territoryName} was auto-rolled — ${defenderName}’s window expired`;
  }
}

export interface RevealPlan {
  /** `shake` is the cup animation; `fade` honours `prefers-reduced-motion`. */
  mode: "shake" | "fade" | "none";
  durationMs: number;
}

/**
 * How to reveal recorded dice. Faces come from the event either way — motion only
 * ever suggests a shake, and never displays a value that was not rolled.
 *
 * `alreadySeen` is what makes a reconnect honest: a result the client is learning
 * about for the first time *because it reloaded* is shown, not re-animated.
 */
export function revealPlan(options: { reducedMotion: boolean; alreadySeen: boolean }): RevealPlan {
  if (options.alreadySeen) return { mode: "none", durationMs: 0 };
  if (options.reducedMotion) return { mode: "fade", durationMs: 180 };
  return { mode: "shake", durationMs: 620 };
}

// ---------------------------------------------------------------------------
// Current-turn ledger
// ---------------------------------------------------------------------------

export interface LedgerEntry {
  id: string;
  icon: string;
  text: string;
  detail?: string;
}

const plural = (count: number, one: string, many = `${one}s`): string =>
  `${count} ${count === 1 ? one : many}`;

/**
 * `"⚄ 6 · 5 · 2 vs 4 · 3 — Ashfell held, 1 attacker lost"`.
 *
 * A defence nobody clicked is named as such, so the dice line under a ledger or
 * history row carries the same truth as the combat card did while it was live.
 */
export function diceOutcomeText(dice: ProjectedDiceV2, names: NameLookup): string {
  const losses = [
    dice.attackerLosses > 0 ? `${plural(dice.attackerLosses, "attacker")} lost` : null,
    dice.defenderLosses > 0 ? `${plural(dice.defenderLosses, "defender")} lost` : null,
  ].filter((part): part is string => part !== null);
  const outcome = dice.territoryCaptured
    ? `${names.territory(dice.to)} captured`
    : losses.length > 0
      ? losses.join(", ")
      : "no losses";
  const authorised =
    dice.resolutionSource === "human" ? "" : ` · ${AUTO_ROLL_LABELS[dice.resolutionSource]}`;
  return `${dice.attackerRolls.join(" · ")} vs ${dice.defenderRolls.join(" · ")} — ${outcome}${authorised}`;
}

/**
 * The current turn in human product language, in the order a turn happens.
 *
 * Derived wholly from the projection's `turn` row: counters, the reinforcement
 * breakdown, and the latest recorded throw. Nothing here needs the move feed.
 */
export function turnLedger(turn: ProjectedTurnV2, names: NameLookup): LedgerEntry[] {
  const entries: LedgerEntry[] = [];
  const { reinforcement } = turn;

  if (reinforcement.total > 0) {
    entries.push({
      id: "reinforce",
      icon: "▲",
      text:
        turn.reinforcementsPlaced === 0
          ? `${reinforcement.total} reinforcements to place`
          : `Placed ${turn.reinforcementsPlaced} of ${reinforcement.total} reinforcements`,
      detail: reinforcementEquation(reinforcement, names.continent),
    });
  }

  if (turn.attacksDeclared > 0) {
    entries.push({
      id: "attacks",
      icon: "⚔",
      text: `Declared ${plural(turn.attacksDeclared, "attack")}`,
      detail:
        turn.throwsResolved === turn.attacksDeclared
          ? `${plural(turn.throwsResolved, "throw")} resolved`
          : `${turn.throwsResolved} of ${turn.attacksDeclared} throws resolved`,
    });
  }

  if (turn.latestDice) {
    entries.push({
      id: `dice:${turn.latestDice.attackId}`,
      icon: "⚄",
      text: `${names.territory(turn.latestDice.from)} → ${names.territory(turn.latestDice.to)}`,
      detail: diceOutcomeText(turn.latestDice, names),
    });
  }

  if (turn.captures > 0) {
    entries.push({
      id: "captures",
      icon: "★",
      text: `Captured ${plural(turn.captures, "country", "countries")}`,
    });
  }

  if (turn.eliminations > 0) {
    entries.push({
      id: "eliminations",
      icon: "☠",
      text: `Eliminated ${plural(turn.eliminations, "player")}`,
    });
  }

  if (turn.phase === "fortify") {
    entries.push({ id: "fortify", icon: "⇢", text: "Fortified — only ending the turn remains" });
  }

  if (entries.length === 0) {
    entries.push({ id: "idle", icon: "·", text: "Nothing has happened yet this turn" });
  }
  return entries;
}

// ---------------------------------------------------------------------------
// Game history
// ---------------------------------------------------------------------------

/**
 * One past move in product language. Deliberately free of source offsets: the
 * compact sync pill is the only architectural status the game surface keeps
 * (design spec §8.4).
 */
export function moveTextV2(move: ProjectedMoveV2, names: NameLookup): string {
  const who = names.player(move.playerId);
  switch (move.kind) {
    case "GameCreated":
      return `${who} opened the lobby`;
    case "PlayerJoined":
      return `${who} joined the game`;
    case "GameStarted":
      return "Countries dealt — the campaign begins";
    case "ArmiesReinforced":
      return `${who} reinforced ${names.territory(move.territoryId ?? "")} with ${move.armies}`;
    case "AttackDeclared":
      return `${who} attacked ${names.territory(move.from ?? "")} → ${names.territory(move.to ?? "")}`;
    case "AttackResolved":
      // `playerId` on this row is the *defender* — the person the copy is about.
      return defenseAttribution(
        move.resolutionSource ?? "human",
        who,
        names.territory(move.to ?? ""),
      );
    case "TerritoryOccupied":
      return `${who} occupied ${names.territory(move.to ?? "")} with ${move.armies}`;
    case "ArmiesFortified":
      return `${who} moved ${move.armies} armies ${names.territory(move.from ?? "")} → ${names.territory(move.to ?? "")}`;
    case "PlayerEliminated":
      return `${who} was eliminated`;
    case "TurnEnded":
      return `${who} ended their turn`;
    case "GameWon":
      return `${who} conquered the map`;
  }
}

/** The dice line under a history row, when that row recorded a throw. */
export function moveDetailV2(move: ProjectedMoveV2, names: NameLookup): string | null {
  if (move.kind !== "AttackResolved" || !move.attackerRolls || !move.defenderRolls) return null;
  return diceOutcomeText(
    {
      attackId: move.attackId ?? "",
      from: move.from ?? "",
      to: move.to ?? "",
      attackerRolls: move.attackerRolls,
      defenderRolls: move.defenderRolls,
      attackerLosses: move.attackerLosses ?? 0,
      defenderLosses: move.defenderLosses ?? 0,
      territoryCaptured: move.territoryCaptured ?? false,
      resolutionSource: move.resolutionSource ?? "human",
    },
    names,
  );
}

// ---------------------------------------------------------------------------
// Map details card
// ---------------------------------------------------------------------------

export const TERRAIN_LABELS: Record<Terrain, string> = {
  plains: "Plains",
  forest: "Forest",
  hills: "Hills",
  desert: "Desert",
  mountains: "Mountains",
};

/** `"3 Forest · 2 Hills"` — terrain is visual-only in v2, so this is character, not maths. */
export function terrainMix(terrains: readonly Terrain[]): string {
  const counts = new Map<Terrain, number>();
  for (const terrain of terrains) counts.set(terrain, (counts.get(terrain) ?? 0) + 1);
  return [...counts.entries()]
    .toSorted((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([terrain, count]) => `${count} ${TERRAIN_LABELS[terrain]}`)
    .join(" · ");
}

// ---------------------------------------------------------------------------
// Lobby tooling
// ---------------------------------------------------------------------------

/**
 * The copy-pasteable command that brings an agent seat online.
 *
 * `BASE_URL` is not optional in practice: the harness defaults to port 1339, and
 * this demo's server takes its port from `$PORT`, so a printed command without an
 * origin fails for anyone who did not happen to run on the default (D1). The
 * server that served this page knows where it is, so the origin comes from there
 * rather than from the player.
 */
export function agentHarnessCommand(options: {
  origin: string;
  gameId: string;
  playerId: string;
  token: string;
  cursorFile?: string;
}): string {
  const origin = options.origin.replace(/\/+$/, "");
  return [
    `BASE_URL=${origin}`,
    `GAME_ID=${options.gameId}`,
    `PLAYER_ID=${options.playerId}`,
    `PLAYER_TOKEN=${options.token}`,
    ...(options.cursorFile ? [`CURSOR_FILE=${options.cursorFile}`] : []),
    "bun run --cwd examples/risk-demo agent",
  ].join(" ");
}

// ---------------------------------------------------------------------------
// Status language
// ---------------------------------------------------------------------------

export type SeatMode = "active-turn" | "defense" | "waiting" | "finished";

/** The short "what is being asked of me" line at the top of the rail. */
export function seatStatusLabel(options: {
  spectating: boolean;
  mode: SeatMode | null;
  activePlayerName: string;
  finished: boolean;
  winnerName?: string;
}): string {
  if (options.finished) {
    return options.winnerName ? `${options.winnerName} wins the map` : "Game over";
  }
  if (options.spectating) return "Spectating live";
  switch (options.mode) {
    case "defense":
      return "Defend your country";
    case "active-turn":
      return "Your turn";
    default:
      return `Waiting for ${options.activePlayerName}`;
  }
}
