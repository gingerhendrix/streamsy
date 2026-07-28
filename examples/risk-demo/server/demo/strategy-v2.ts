/**
 * The `risk-demo-v2` scripted-bot policy, kept pure and away from HTTP.
 *
 * It is deliberately shallow — no search, no evaluation of an opponent's reply —
 * but it does have to satisfy one structural property:
 *
 *   **a turtling opponent must not be able to freeze the game.**
 *
 * A human who never attacks and stacks one fortress must not draw all
 * reinforcements to that border. Armies need to flow toward favourable attacks
 * elsewhere on the map.
 *
 * Three rules turn armies into attacks rather than a wall:
 *
 *  - **Reinforce toward weakness, not toward danger.** A border is scored by the
 *    weakest enemy it touches and by whether this turn's pool is enough to make
 *    an attack there legal and favourable — not by how many enemies it faces.
 *  - **Attack from anywhere the odds are favourable**, ignoring how large some
 *    other enemy stack elsewhere on the map happens to be.
 *  - **Fortify unstuck.** A stack sitting on a border it cannot attack out of is
 *    as idle as an interior garrison, so it is a legal fortify source too, and
 *    destinations are ranked by the attack they unlock.
 *
 * Continent value breaks ties, but never outranks being able to move at all.
 */

/** Ownership and armies as `/decision` reports them. */
export interface StrategyTerritory {
  id: string;
  ownerId?: string;
  armies: number;
}

/** Static geometry as `/board` reports it; immutable after `GameStarted`. */
export interface StrategyMap {
  territories: Array<{ id: string; continentId: string; adjacentTerritoryIds: string[] }>;
  continents: Array<{ id: string; territoryIds: string[]; reinforcementBonus: number }>;
}

export interface ReinforceAction {
  territoryIds: string[];
  pool: number;
}
export interface AttackChoice {
  from: string;
  to: string;
  maxAttackerDice: number;
}
export interface AttackAction {
  choices: AttackChoice[];
}
export interface OccupyAction {
  attackId: string;
  from: string;
  to: string;
  minArmies: number;
  maxArmies: number;
}
export interface FortifyAction {
  choices: Array<{ from: string; reachable: Array<{ to: string; maxArmies: number }> }>;
}

export interface StrategyContext {
  playerId: string;
  map: StrategyMap;
  armiesOf(id: string): number;
  ownerOf(id: string): string | undefined;
  neighbours(id: string): string[];
}

export function strategyContext(
  playerId: string,
  territories: readonly StrategyTerritory[],
  map: StrategyMap,
): StrategyContext {
  const byId = new Map(territories.map((territory) => [territory.id, territory]));
  const adjacency = new Map(map.territories.map((t) => [t.id, t.adjacentTerritoryIds]));
  return {
    playerId,
    map,
    armiesOf: (id) => byId.get(id)?.armies ?? 0,
    ownerOf: (id) => byId.get(id)?.ownerId,
    neighbours: (id) => adjacency.get(id) ?? [],
  };
}

/** Enemy countries bordering `id`. The bot's whole notion of "exposed". */
export function enemyNeighbours(ctx: StrategyContext, id: string): string[] {
  return ctx.neighbours(id).filter((adj) => ctx.ownerOf(adj) !== ctx.playerId);
}

/**
 * How much the bot cares about a continent: full control is worth defending,
 * and being one country away is worth pushing for.
 */
export function continentPressure(ctx: StrategyContext, territoryId: string): number {
  const territory = ctx.map.territories.find((t) => t.id === territoryId);
  if (!territory) return 0;
  const continent = ctx.map.continents.find((c) => c.id === territory.continentId);
  if (!continent) return 0;
  const missing = continent.territoryIds.filter((id) => ctx.ownerOf(id) !== ctx.playerId).length;
  if (missing === 0) return continent.reinforcementBonus;
  if (missing === 1) return Math.max(1, Math.floor(continent.reinforcementBonus / 2));
  return 0;
}

/** Armies on the softest enemy touching `id`; `Infinity` when it borders none. */
export function weakestEnemyArmies(ctx: StrategyContext, id: string): number {
  return enemyNeighbours(ctx, id).reduce(
    (weakest, enemy) => Math.min(weakest, ctx.armiesOf(enemy)),
    Infinity,
  );
}

/**
 * The best army difference an attack out of `id` could have right now; `-Infinity`
 * when no attack is possible from it at all.
 *
 * One army must stay behind, so a country of `n` armies attacks with `n - 1`.
 */
export function bestAdvantageFrom(ctx: StrategyContext, id: string): number {
  const weakest = weakestEnemyArmies(ctx, id);
  if (weakest === Infinity || ctx.armiesOf(id) < 2) return -Infinity;
  return ctx.armiesOf(id) - 1 - weakest;
}

/** Armies this country still needs before an attack out of it is worth making. */
export function armiesNeededToAttack(ctx: StrategyContext, id: string): number {
  const weakest = weakestEnemyArmies(ctx, id);
  if (weakest === Infinity) return Infinity;
  return Math.max(0, weakest + 2 - ctx.armiesOf(id));
}

const byId = (a: { id: string }, b: { id: string }): number => (a.id < b.id ? -1 : 1);

/**
 * Place the pool where it buys an attack.
 *
 * The ranking is: borders whose weakest enemy this pool can actually overcome,
 * then the softest enemy available, then continent value, then the thinnest
 * garrison. Piling onto the border that faces the biggest enemy stack — the old
 * "most exposed" rule — is precisely what produced a frozen game.
 */
export function chooseReinforce(
  ctx: StrategyContext,
  action: ReinforceAction,
): Record<string, unknown> {
  const pool = action.pool;
  const scored = action.territoryIds
    .map((id) => ({
      id,
      weakest: weakestEnemyArmies(ctx, id),
      needed: armiesNeededToAttack(ctx, id),
      pressure: continentPressure(ctx, id),
      armies: ctx.armiesOf(id),
    }))
    .filter((candidate) => candidate.weakest !== Infinity);

  const best =
    scored.length === 0
      ? null
      : scored.toSorted(
          (a, b) =>
            Number(b.needed <= pool) - Number(a.needed <= pool) ||
            a.weakest - b.weakest ||
            b.pressure - a.pressure ||
            a.armies - b.armies ||
            byId(a, b),
        )[0]!;

  // With no border at all (every country interior), any placement is equivalent.
  return {
    type: "reinforce",
    placements: [{ territoryId: best?.id ?? action.territoryIds[0], armies: pool }],
  };
}

/**
 * Attack wherever the local odds are favourable, regardless of what the enemy
 * holds elsewhere. Continent value breaks ties; the biggest army difference wins
 * the rest, which naturally sends throws at the softest target in reach.
 */
export function chooseAttack(
  ctx: StrategyContext,
  action: AttackAction,
): Record<string, unknown> | null {
  const best = action.choices
    .map((choice) => ({
      choice,
      advantage: ctx.armiesOf(choice.from) - 1 - ctx.armiesOf(choice.to),
      pressure: continentPressure(ctx, choice.to),
    }))
    .filter((candidate) => candidate.advantage >= 1)
    .toSorted(
      (a, b) =>
        b.pressure - a.pressure ||
        b.advantage - a.advantage ||
        (a.choice.to < b.choice.to ? -1 : 1),
    )[0];
  if (!best) return null;
  return {
    type: "declare-attack",
    from: best.choice.from,
    to: best.choice.to,
    attackerDice: best.choice.maxAttackerDice,
  };
}

/**
 * Occupy with the minimum, unless the captured country still borders enemies —
 * then push a bounded share of the available garrison forward instead of leaving
 * a token holding to be retaken next turn.
 */
export function chooseOccupy(ctx: StrategyContext, action: OccupyAction): Record<string, unknown> {
  const exposed = enemyNeighbours(ctx, action.to).length;
  const armies =
    exposed > 0
      ? Math.min(
          action.maxArmies,
          Math.max(action.minArmies, Math.ceil((action.minArmies + action.maxArmies) / 2)),
        )
      : action.minArmies;
  return { type: "occupy-territory", attackId: action.attackId, armies };
}

/** A border stack keeps a token garrison behind; an interior one keeps one army. */
const BORDER_GARRISON = 3;

/**
 * Move idle armies to where they can be spent.
 *
 * "Idle" is the load-bearing word. An interior country is idle because it faces
 * nobody; a border country whose every enemy neighbour is unbeatable is idle for
 * the same practical reason, and refusing to move it is what let a single enemy
 * fortress absorb the bot's whole army forever. Both are sources here, largest
 * first, and destinations are ranked by the attack the arriving armies unlock.
 */
export function chooseFortify(
  ctx: StrategyContext,
  action: FortifyAction,
): Record<string, unknown> | null {
  const sources = action.choices
    .map((choice) => {
      const border = enemyNeighbours(ctx, choice.from).length > 0;
      const armies = ctx.armiesOf(choice.from);
      // A stack that can already attack is not idle — leave it where it is.
      const stuck = border && bestAdvantageFrom(ctx, choice.from) < 1;
      const movable = border ? armies - Math.min(BORDER_GARRISON, armies - 1) : armies - 1;
      return { choice, border, stuck, movable };
    })
    .filter((source) => (source.border ? source.stuck : true) && source.movable >= 1)
    .toSorted(
      (a, b) =>
        Number(a.border) - Number(b.border) ||
        b.movable - a.movable ||
        (a.choice.from < b.choice.from ? -1 : 1),
    );

  for (const source of sources) {
    const destinations = source.choice.reachable
      .map((reachable) => ({
        reachable,
        weakest: weakestEnemyArmies(ctx, reachable.to),
        pressure: continentPressure(ctx, reachable.to),
      }))
      .filter((candidate) => candidate.weakest !== Infinity)
      .toSorted(
        (a, b) =>
          a.weakest - b.weakest ||
          b.pressure - a.pressure ||
          ctx.armiesOf(a.reachable.to) - ctx.armiesOf(b.reachable.to) ||
          (a.reachable.to < b.reachable.to ? -1 : 1),
      );

    const target = destinations[0];
    if (!target) continue;
    // Never trade one stuck stack for another: only move toward a border whose
    // weakest enemy is softer than the one this source is already facing.
    if (source.border && target.weakest >= weakestEnemyArmies(ctx, source.choice.from)) continue;
    const armies = Math.min(target.reachable.maxArmies, source.movable);
    if (armies < 1) continue;
    return { type: "fortify", from: source.choice.from, to: target.reachable.to, armies };
  }
  return null;
}
