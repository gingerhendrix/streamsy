import { describe, expect, it } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  directProtocolClient,
} from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";
import { catchUp } from "./mesh-test-harness.ts";

import { createJsonProtocol } from "@streamsy/json";

import { foldAggregate } from "../domain/aggregate.ts";
import type { GameEvent } from "../domain/events.ts";
import {
  ProjectionIntegrityError,
  aggregateBoardView,
  boardsEqual,
  initialProjection,
  projectEvent,
  projectionBoardView,
  type ProjectionState,
} from "./projection.ts";
import { writeCanonicalEvents, BOARD_REDUCER_VERSION } from "./board-projection.ts";
import { createBoardMesh, type BoardMaterialized } from "./mesh.ts";
import { createLineageEvent, type CatchUpLimits } from "@streamsy/experimental/ivm-mesh";
import { boardProjectionTxId } from "./transaction.ts";
import {
  armForAttack,
  declareAttack,
  nextCommandId,
  occupyPending,
  startGame,
  throwUntilCapture,
  winThrow,
  type ScriptedGame,
} from "../../test/testkit.ts";

const SOURCE = "games/game/events";
const OUTPUT = "games/game/projections/board/board1";

function newProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
}

/**
 * One bounded catch-up of a board generation, returning the state the mesh
 * recovered from the durable board rather than anything held in this process.
 */
async function runBoard(
  protocol: StreamProtocolFactory,
  generation = "board1",
  limits?: CatchUpLimits,
) {
  const outputStreamId = `games/game/projections/board/${generation}`;
  await protocol.create(outputStreamId, { contentType: "application/json" });
  const client = directProtocolClient(protocol);
  const mesh = await createBoardMesh({
    gameId: "game",
    client,
    sourceStreamId: SOURCE,
    outputStreamId,
    generation,
    ...(limits ? { limits } : {}),
  });
  const result = await catchUp<GameEvent, BoardMaterialized>({
    source: mesh.source,
    target: mesh.target,
    lane: mesh.lane,
    limits: mesh.limits,
    fold: mesh.fold,
    decode: (batch) => mesh.decode(batch),
    reduce: (events, boundary, prior) => mesh.reduce(events, boundary, prior),
  }).finally(() => client.close());
  if (result.status !== "caught-up" && result.status !== "limit-reached") {
    throw new Error(`board catch-up: ${result.status}`);
  }
  return { result, materialized: result.checkpoint.materialized, mesh, outputStreamId };
}

/** Any owned country with a spare army and an enemy neighbour, or null. */
function findAttack(game: ScriptedGame) {
  const state = game.state();
  const active = state.activePlayerId!;
  for (const territory of Object.values(state.territories).toSorted((a, b) =>
    a.id.localeCompare(b.id),
  )) {
    if (territory.ownerId !== active || territory.armies < 2) continue;
    const to = state
      .index!.territoryById.get(territory.id)!
      .adjacentTerritoryIds.find((adj) => state.territories[adj]!.ownerId !== active);
    if (to) {
      return {
        from: territory.id,
        to,
        attackerId: active,
        defenderId: state.territories[to]!.ownerId!,
      };
    }
  }
  return null;
}

/**
 * Reinforce → attack (always winning) → occupy → skip fortifications, for up to `maxSteps`
 * decisions. Whoever is active plays, so both seats take real turns.
 */
function drive(game: ScriptedGame, maxSteps: number): readonly GameEvent[] {
  for (let step = 0; step < maxSteps; step += 1) {
    const state = game.state();
    if (state.status !== "playing") return game.log;
    if (state.pendingInteraction?.type === "occupation") {
      occupyPending(game);
      continue;
    }
    if (state.phase === "reinforce") {
      armForAttack(game);
      continue;
    }
    const setup = findAttack(game);
    if (setup) {
      winThrow(game, setup);
      continue;
    }
    game.must({
      type: "skip-fortifications",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: state.activePlayerId!,
    });
  }
  return game.log;
}

/**
 * A game that runs all the way to `GameWon`.
 *
 * Rigged dice alone do not converge — both seats sweep their own throws, so
 * ownership just oscillates — so the canonical allocation is rewritten to leave
 * the second player a two-country holding. The result is still an ordinary
 * `GameStarted`, folded by both reducers exactly as in a real game, and it
 * exercises capture, elimination, and victory in one log.
 */
function playFullGame(mapSeed: string): readonly GameEvent[] {
  const game = startGame({
    players: 2,
    mapSeed,
    board: ({ map, turnOrder }) => {
      const [winner, loser] = turnOrder;
      const ids = map.territories.map((t) => t.id);
      return {
        initialTerritories: ids.map((territoryId, index) => ({
          territoryId,
          ownerId: index >= ids.length - 2 ? loser! : winner!,
          armies: index >= ids.length - 2 ? 1 : 3,
        })),
      };
    },
  });
  const log = drive(game, 400);
  if (game.state().status !== "finished") throw new Error("scripted current game did not finish");
  if (!log.some((event) => event.type === "PlayerEliminated")) {
    throw new Error("scripted current game did not eliminate anyone");
  }
  return log;
}

/** A long, ordinary two-sided game — many turns, captures, and turn resets. */
function playLongGame(mapSeed: string, steps = 220): readonly GameEvent[] {
  return drive(startGame({ players: 3, mapSeed }), steps);
}

describe("Hex Domination board projection reducer", () => {
  it.each([
    ["a game played through to victory", () => playFullGame("projection-equivalence")],
    ["a long three-player game", () => playLongGame("projection-long")],
  ])("agrees with the aggregate fold at every prefix of %s", (_label, build) => {
    const events = build();
    expect(events.length).toBeGreaterThan(8);

    let projection = initialProjection();
    for (let index = 0; index < events.length; index += 1) {
      projection = projectEvent(projection, events[index]!, String(index), index);
      const authoritative = aggregateBoardView(foldAggregate(events.slice(0, index + 1)));
      const projected = projectionBoardView(projection);
      if (!boardsEqual(projected, authoritative)) {
        // Surface the offending prefix rather than a bare `false`.
        expect({ index, kind: events[index]!.type, projected }).toEqual({
          index,
          kind: events[index]!.type,
          projected: authoritative,
        });
      }
    }
  });

  it("carries the pending defence interrupt, including the recorded attacker dice", () => {
    const game = startGame({ players: 2, mapSeed: "pending-defence" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    const attackId = declareAttack(game, setup);

    const projection = replay(game.log);
    const view = projectionBoardView(projection);
    expect(view.pending?.type).toBe("defense");
    expect(boardsEqual(view, aggregateBoardView(game.state()))).toBe(true);

    expect(projection.combat).toMatchObject({
      attackId,
      status: "awaiting-defense",
      attackerRolls: [6, 6, 6],
    });
    expect(projection.turn?.attacksDeclared).toBe(1);
  });

  it("carries the pending occupation bounds and clears combat once occupied", () => {
    const game = startGame({ players: 2, mapSeed: "pending-occupation" });
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);

    const captured = replay(game.log);
    expect(captured.combat).toMatchObject({
      status: "awaiting-occupation",
      minArmies: pending.minArmies,
      maxArmies: pending.maxArmies,
      territoryCaptured: true,
    });
    expect(boardsEqual(projectionBoardView(captured), aggregateBoardView(game.state()))).toBe(true);

    occupyPending(game, pending.maxArmies);
    const occupied = replay(game.log);
    expect(occupied.combat).toBeNull();
    expect(occupied.turn?.captures).toBe(1);
    expect(occupied.territories.find((t) => t.id === pending.to)!.ownerId).toBe(pending.playerId);
    expect(boardsEqual(projectionBoardView(occupied), aggregateBoardView(game.state()))).toBe(true);
  });

  it("projects the canonical map snapshot verbatim and derives nothing about geometry", () => {
    const game = startGame({ players: 3, mapSeed: "map-rows" });
    const started = game.log.find((event) => event.type === "GameStarted");
    if (started?.type !== "GameStarted") throw new Error("no GameStarted");
    const projection = replay(game.log);

    expect(projection.hexes.map((h) => h.id)).toEqual(started.map.tiles.map((t) => t.id));
    expect(projection.territories.map((t) => t.adjacentTerritoryIds)).toEqual(
      started.map.territories.map((t) => [...t.adjacentTerritoryIds]),
    );
    expect(projection.continents.map((c) => c.territoryIds)).toEqual(
      started.map.continents.map((c) => [...c.territoryIds]),
    );
    expect(projection.territories.map((t) => t.labelAnchor)).toEqual(
      started.map.territories.map((t) => ({ ...t.labelAnchor })),
    );
  });

  it("tracks the current-turn reinforcement breakdown and resets it on TurnEnded", () => {
    const game = startGame({ players: 2, mapSeed: "turn-row" });
    const before = replay(game.log);
    const first = before.turn!;
    expect(first.reinforcement.total).toBe(first.reinforcement.base + bonusSum(first));
    expect(first.reinforcementsPlaced).toBe(0);
    expect(first.reinforcement.remaining).toBe(first.reinforcement.total);

    armForAttack(game);
    const placed = replay(game.log).turn!;
    expect(placed.reinforcementsPlaced).toBe(first.reinforcement.total);
    expect(placed.reinforcement.remaining).toBe(0);
    expect(placed.phase).toBe("attack");

    game.must({
      type: "skip-fortifications",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: game.state().activePlayerId!,
    });
    const next = replay(game.log).turn!;
    expect(next.turnId).not.toBe(first.turnId);
    expect(next.reinforcementsPlaced).toBe(0);
    expect(next.attacksDeclared).toBe(0);
  });

  it("fails loudly when an AttackResolved contradicts its declaration", () => {
    const game = startGame({ players: 2, mapSeed: "integrity" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    declareAttack(game, setup);
    game.rig([1, 1]);
    game.must({
      type: "roll-defense",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: setup.defenderId,
      attackId: game.log.findLast((e) => e.type === "AttackDeclared")!.attackId,
    });

    const events = game.log.slice();
    const resolvedIndex = events.findIndex((event) => event.type === "AttackResolved");
    const resolved = events[resolvedIndex]!;
    if (resolved.type !== "AttackResolved") throw new Error("unreachable");
    // Rewrite the repeated attacker rolls: the projection must not fold this.
    events[resolvedIndex] = { ...resolved, attackerRolls: [1, 1, 1] };

    expect(() => replay(events)).toThrow(ProjectionIntegrityError);
  });
});

function bonusSum(turn: { reinforcement: { continents: Array<{ bonus: number }> } }): number {
  return turn.reinforcement.continents.reduce((sum, c) => sum + c.bonus, 0);
}

function replay(events: readonly GameEvent[]): ProjectionState {
  let state = initialProjection("game");
  for (let index = 0; index < events.length; index += 1) {
    state = projectEvent(state, events[index]!, String(index), index);
  }
  return state;
}

describe("Hex Domination board projection materializer", () => {
  it("materializes a full game so the projection equals the aggregate at the source head", async () => {
    const events = playFullGame("materialize-full");
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);

    const { materialized } = await runBoard(protocol);

    expect(materialized.sourceSeq).toBe(events.length - 1);
    expect(
      boardsEqual(
        projectionBoardView(materialized.state),
        aggregateBoardView(foldAggregate(events)),
      ),
    ).toBe(true);
    expect(materialized.state.game.status).toBe("finished");
    expect(materialized.state.combat).toBeNull();
  });

  it("names every change after the command that produced it", async () => {
    const events = playFullGame("materialize-txid").slice(0, 6);
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);
    await runBoard(protocol);

    const output = await protocol.get(OUTPUT);
    if (output.status !== "ok") throw new Error("no projection stream");
    const read = await output.stream.read({});
    if (read.status !== "ok") throw new Error("cannot read projection stream");
    const messages = read.messages.map(
      (message) =>
        JSON.parse(new TextDecoder().decode(message.data)) as {
          type?: string;
          headers?: { txid?: string };
        },
    );
    // The lineage row is the framework's and carries no application txid; the
    // last application row is the checkpoint, named after the final command.
    expect(messages.at(-1)?.type).toBe("__streamsy.mesh.lineage.v1");
    const applicationRows = messages.filter((m) => m.type !== "__streamsy.mesh.lineage.v1");
    expect(applicationRows.at(-1)?.headers?.txid).toBe(
      boardProjectionTxId(events.at(-1)!.commandId),
    );
    // Every command in the log is waitable: each one names a transaction.
    const seen = new Set(applicationRows.map((row) => row.headers?.txid));
    for (const event of events) {
      expect(seen.has(boardProjectionTxId(event.commandId))).toBe(true);
    }
  });

  it("rebuilds a byte-identical generation, including combat and watermark", async () => {
    // Stop mid-combat so the rebuild has a pending interrupt to reproduce.
    const game = startGame({ players: 2, mapSeed: "rebuild-pending" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    declareAttack(game, setup);

    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, game.log);

    const first = await runBoard(protocol, "board1");
    const rebuilt = await runBoard(protocol, "board2");

    expect(rebuilt.materialized.state).toEqual(first.materialized.state);
    expect(rebuilt.materialized.state.sourceThroughOffset).toBe(
      first.materialized.state.sourceThroughOffset,
    );
    expect(rebuilt.materialized.state.combat?.status).toBe("awaiting-defense");
  });

  it("catches up incrementally as canonical events arrive, gap-free", async () => {
    const events = playFullGame("materialize-incremental");
    const protocol = newProtocol();

    const midpoint = Math.floor(events.length / 2);
    await writeCanonicalEvents(protocol, SOURCE, events.slice(0, midpoint));
    const first = await runBoard(protocol);
    expect(first.result.items).toBe(midpoint);
    // The mid-game snapshot is itself a valid board.
    expect(
      boardsEqual(
        projectionBoardView(first.materialized.state),
        aggregateBoardView(foldAggregate(events.slice(0, midpoint))),
      ),
    ).toBe(true);

    await writeCanonicalEvents(protocol, SOURCE, events.slice(midpoint));
    const second = await runBoard(protocol);
    expect(second.result.items).toBe(events.length - midpoint);
    expect(
      boardsEqual(
        projectionBoardView(second.materialized.state),
        aggregateBoardView(foldAggregate(events)),
      ),
    ).toBe(true);
  });

  it("recovers the board and watermark from the projection stream alone", async () => {
    const events = playFullGame("materialize-reload");
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);
    await runBoard(protocol);

    // A second catch-up has no new source to read, so everything it reports
    // came out of the durable board.
    const reloaded = await runBoard(protocol);
    expect(reloaded.result.items).toBe(0);
    expect(
      boardsEqual(
        projectionBoardView(reloaded.materialized.state),
        aggregateBoardView(foldAggregate(events)),
      ),
    ).toBe(true);
    expect(reloaded.materialized.state.sourceThroughOffset).not.toBeNull();
    expect(reloaded.result.checkpoint.sourceThrough).toBeDefined();
  });
});

/**
 * The acknowledgement contract's load-bearing assumption.
 *
 * `boardProjectionTxId` names a transaction after a command, which is only exact
 * if every event a command emits lands in one State transaction. That holds
 * because a command appends its events atomically and a catch-up read returns
 * whole messages up to the durable head, so a delivery boundary can only end
 * where an append ended. It is an invariant of the transport, not of this
 * package, so it is asserted here rather than assumed.
 */
describe("command-to-transaction boundary", () => {
  it("keeps every event of a multi-event command inside one State transaction", async () => {
    // A full game contains commands whose `decide` emits several events at once
    // (a resolved throw that captures, an occupation that ends a turn).
    const events = playFullGame("one-transaction");
    const byCommand = groupByCommand(events);
    const commands = [...byCommand.values()];
    expect(commands.some((group) => group.length > 1)).toBe(true);

    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);

    // Append the way the command log does — one atomic batch per command — and
    // catch up in between, so the board really is built from many transactions.
    for (const group of commands) {
      const appended = await stream.appendBatch(group);
      if (appended.status !== "appended") throw new Error(`append: ${appended.status}`);
      await runBoard(protocol);
    }

    const transactions = await readTransactions(protocol);
    expect(transactions.length).toBe(commands.length);

    // No command's transaction id appears in two transactions: a command's
    // effects are never split, so waiting on its txid cannot observe a
    // half-applied command.
    const seen = countTxIds(transactions);
    for (const [txid, count] of seen) {
      expect({ txid, count }).toEqual({ txid, count: 1 });
    }
    for (const commandId of byCommand.keys()) {
      expect(seen.get(boardProjectionTxId(commandId))).toBe(1);
    }
  });

  it("folds a backlog of several commands into one transaction, still naming each", async () => {
    // The converse of the guarantee above does *not* hold. When a projector is
    // behind, one delivery boundary covers everything unread, so several
    // commands share a transaction. Waiting stays sound because every command
    // in it is applied completely.
    const events = playFullGame("backlog").slice(0, 12);
    const byCommand = groupByCommand(events);
    const commands = [...byCommand.values()];
    expect(commands.length).toBeGreaterThan(1);

    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    for (const group of commands) {
      const appended = await stream.appendBatch(group);
      if (appended.status !== "appended") throw new Error(`append: ${appended.status}`);
    }
    // One catch-up over the whole backlog.
    await runBoard(protocol);

    const transactions = await readTransactions(protocol);
    expect(transactions.length).toBe(1);
    // Every command is still individually waitable inside that one transaction.
    const seen = countTxIds(transactions);
    for (const commandId of byCommand.keys()) {
      expect(seen.get(boardProjectionTxId(commandId))).toBe(1);
    }
  });
});

/**
 * Move rows are keyed by canonical event ordinal, not by source offset.
 *
 * A delivery boundary gives every event in it one offset, so an offset-keyed
 * move row collides whenever a command emits more than one event — and the
 * collision is silent and asymmetric: the state snapshot keeps every move,
 * while the row set keyed by id keeps only the last. The browser mirror would
 * then disagree with the board it mirrors, with no error anywhere.
 */
describe("move row keys", () => {
  it("gives each event of a multi-event command a distinct move key", async () => {
    const events = playFullGame("move-keys");
    const byCommand = groupByCommand(events);
    expect([...byCommand.values()].some((group) => group.length > 1)).toBe(true);

    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    // One boundary covering everything: the worst case for offset-keyed moves.
    for (const group of byCommand.values()) await stream.appendBatch(group);
    const { materialized } = await runBoard(protocol);

    const ids = materialized.state.moves.map((move) => move.id);
    expect(new Set(ids).size).toBe(ids.length);
    // Several moves legitimately share a source offset — that is the watermark,
    // not the key — which is exactly why the key had to become something else.
    const offsets = new Set(materialized.state.moves.map((move) => move.sourceOffset));
    expect(offsets.size).toBeLessThan(ids.length);
    // Keys sort in canonical event order.
    expect(ids).toEqual(ids.toSorted());
  });

  it("keeps the snapshot and the mirrored row set in agreement", async () => {
    const events = playFullGame("mirror-parity");
    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    for (const group of groupByCommand(events).values()) await stream.appendBatch(group);
    const { materialized } = await runBoard(protocol);

    // Apply the emitted changes the way a keyed consumer does, then compare the
    // resulting move rows with the snapshot's own feed.
    const mirror = new Map<string, unknown>();
    for (const row of await readRows(protocol)) {
      if (row.type !== "move") continue;
      const operation = (row.headers as { operation: string }).operation;
      if (operation === "delete") mirror.delete(row.key as string);
      else mirror.set(row.key as string, row.value);
    }

    expect(mirror.size).toBe(materialized.state.moves.length);
    expect([...mirror.keys()].toSorted()).toEqual(
      materialized.state.moves.map((move) => move.id).toSorted(),
    );
    for (const move of materialized.state.moves) {
      expect(mirror.get(move.id)).toEqual(move);
    }
  });
});

function groupByCommand(events: readonly GameEvent[]): Map<string, GameEvent[]> {
  const byCommand = new Map<string, GameEvent[]>();
  for (const event of events) {
    const group = byCommand.get(event.commandId) ?? [];
    group.push(event);
    byCommand.set(event.commandId, group);
  }
  return byCommand;
}

function canonicalStream(protocol: StreamProtocolFactory) {
  return createJsonProtocol(protocol, {
    encode: (event: GameEvent) => event,
    decode: (value) => value as GameEvent,
  }).getOrCreate(SOURCE);
}

async function readRows(
  protocol: StreamProtocolFactory,
): Promise<{ type: string; key?: unknown; value?: unknown; headers?: unknown }[]> {
  const output = await protocol.get(OUTPUT);
  if (output.status !== "ok") throw new Error("no projection stream");
  const read = await output.stream.read({});
  if (read.status !== "ok") throw new Error("cannot read projection stream");
  return read.messages.map((message) => JSON.parse(new TextDecoder().decode(message.data)));
}

/** Split the row stream into transactions; each ends at the reserved lineage row. */
async function readTransactions(protocol: StreamProtocolFactory): Promise<Set<string>[]> {
  const transactions: Set<string>[] = [];
  let current = new Set<string>();
  for (const row of await readRows(protocol)) {
    if (row.type === "__streamsy.mesh.lineage.v1") {
      transactions.push(current);
      current = new Set<string>();
      continue;
    }
    const txid = (row.headers as { txid?: string } | undefined)?.txid;
    if (txid) current.add(txid);
  }
  return transactions;
}

function countTxIds(transactions: readonly Set<string>[]): Map<string, number> {
  const seen = new Map<string, number>();
  for (const transaction of transactions) {
    for (const txid of transaction) seen.set(txid, (seen.get(txid) ?? 0) + 1);
  }
  return seen;
}

/**
 * Bounded catch-up, and what the board read does with a budget it cannot meet.
 *
 * Worth stating plainly, because it bounds how much of this matters: a catch-up
 * read returns everything unread as a *single* delivery batch, so one `catchUp`
 * invocation here processes exactly one boundary however long the backlog is.
 * `limit-reached` — which needs a second boundary to refuse — is therefore
 * unreachable in this configuration today, and the drain loop in the board read
 * is a guard against a future paginating reader rather than something exercised
 * now. What *is* reachable is a single boundary larger than the budget, and that
 * must fail loudly rather than commit a partial board.
 */
describe("bounded catch-up", () => {
  it("processes a whole backlog as one boundary regardless of its size", async () => {
    const events = playFullGame("bounded-single").slice(0, 12);
    const commands = [...groupByCommand(events).values()];
    expect(commands.length).toBeGreaterThan(2);

    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    for (const group of commands) await stream.appendBatch(group);

    const { result } = await runBoard(protocol);
    expect(result.status).toBe("caught-up");
    // Many commands, many events — one page, one batch, one transaction.
    expect(result.pages).toBe(1);
    expect(result.batches).toBe(1);
  });

  it("refuses a boundary larger than the budget without writing a partial board", async () => {
    const events = playFullGame("bounded-too-large").slice(0, 12);
    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    for (const group of groupByCommand(events).values()) await stream.appendBatch(group);

    const outputStreamId = "games/game/projections/board/board1";
    await protocol.create(outputStreamId, { contentType: "application/json" });
    const client = directProtocolClient(protocol);
    const mesh = await createBoardMesh({
      gameId: "game",
      client,
      sourceStreamId: SOURCE,
      outputStreamId,
      generation: "board1",
      limits: { maxItems: 2, maxPages: 100, maxBatches: 100, maxBytes: 16 * 1024 * 1024 },
    });
    const result = await catchUp<GameEvent, BoardMaterialized>({
      source: mesh.source,
      target: mesh.target,
      lane: mesh.lane,
      limits: mesh.limits,
      fold: mesh.fold,
      decode: (batch) => mesh.decode(batch),
      reduce: (items, boundary, prior) => mesh.reduce(items, boundary, prior),
    }).finally(() => client.close());

    // Terminal, not a partial success — repeating the same configuration could
    // not make progress, so saying so is the only honest answer.
    expect(result.status).toBe("boundary-too-large");
    // And nothing was written: no rows, no lineage, no watermark to mistake for
    // a caught-up board.
    const rows = await readRows(protocol);
    expect(rows).toHaveLength(0);
  });
});

/**
 * Snapshot and lineage must agree *before* the projection resumes.
 *
 * `projectionMeta.sourceThroughOffset` and the reserved lineage row are written
 * in one transaction, so they can only disagree if something went wrong. The
 * dangerous shape is a snapshot that has moved *past* the lineage: recovery
 * resumes from the lineage position while reducing from the snapshot, so the
 * events in between are applied a second time — and the boundary that commits
 * then carries a lineage and a snapshot that agree, which makes the corruption
 * look like a healthy board. Checking after a catch-up cannot catch this; the
 * check has to happen before the source is read.
 */
describe("snapshot and lineage agreement", () => {
  /**
   * Append a transaction whose checkpoint row claims a later position than the
   * lineage row beside it — a snapshot through B, lineage still through A.
   */
  async function skewCheckpoint(
    protocol: StreamProtocolFactory,
    mesh: Awaited<ReturnType<typeof createBoardMesh>>,
    snapshot: ProjectionState,
    lineageThrough: string,
    aheadOffset: string,
    nextProducerSeq: number,
  ) {
    const output = await protocol.get(OUTPUT);
    if (output.status !== "ok") throw new Error("no projection stream");
    const rows = [
      {
        type: "projectionMeta",
        key: "board",
        value: {
          sourceStreamId: SOURCE,
          sourceThroughOffset: aheadOffset,
          sourceSeq: snapshot.moves.length + 1000,
          generation: "board1",
          reducerVersion: BOARD_REDUCER_VERSION,
          snapshot: { ...snapshot, sourceThroughOffset: aheadOffset },
        },
        headers: { operation: "upsert", offset: aheadOffset },
      },
      createLineageEvent(mesh.lane, { sourceThrough: lineageThrough, nextProducerSeq }),
    ];
    const appended = await output.stream.append({
      data: new TextEncoder().encode(JSON.stringify(rows)),
      contentType: "application/json",
    });
    if (appended.status !== "appended") throw new Error(`skew append: ${appended.status}`);
  }

  async function runRaw(protocol: StreamProtocolFactory) {
    const client = directProtocolClient(protocol);
    const mesh = await createBoardMesh({
      gameId: "game",
      client,
      sourceStreamId: SOURCE,
      outputStreamId: OUTPUT,
      generation: "board1",
    });
    return catchUp<GameEvent, BoardMaterialized>({
      source: mesh.source,
      target: mesh.target,
      lane: mesh.lane,
      limits: mesh.limits,
      fold: mesh.fold,
      validateRecovered: (checkpoint) =>
        mesh.validateRecovered({ ...checkpoint, state: checkpoint.materialized }),
      decode: (batch) => mesh.decode(batch),
      reduce: (items, boundary, prior) => mesh.reduce(items, boundary, prior),
    }).finally(() => client.close());
  }

  it("refuses a snapshot ahead of its lineage when there is no unread source", async () => {
    const events = playFullGame("skew-quiet").slice(0, 6);
    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    for (const group of groupByCommand(events).values()) await stream.appendBatch(group);
    const first = await runBoard(protocol);
    const lineageThrough = first.result.checkpoint.sourceThrough!;

    await skewCheckpoint(
      protocol,
      first.mesh,
      first.materialized.state,
      lineageThrough,
      `${lineageThrough}~ahead`,
      first.result.checkpoint.nextProducerSeq + 1,
    );
    const before = await readRows(protocol);

    await expect(runRaw(protocol)).rejects.toMatchObject({ _tag: "StateRestorePoison" });
    // Nothing was written, so the inconsistency is still visible for diagnosis
    // rather than overwritten by a boundary that makes it look resolved.
    expect(await readRows(protocol)).toEqual(before);
  });

  it("refuses even when the unread source would re-apply cleanly", async () => {
    const events = playFullGame("skew-busy");
    const commands = [...groupByCommand(events).values()];
    const protocol = newProtocol();
    const stream = await canonicalStream(protocol);
    for (const group of commands.slice(0, 3)) await stream.appendBatch(group);
    const first = await runBoard(protocol);
    const lineageThrough = first.result.checkpoint.sourceThrough!;

    await skewCheckpoint(
      protocol,
      first.mesh,
      first.materialized.state,
      lineageThrough,
      `${lineageThrough}~ahead`,
      first.result.checkpoint.nextProducerSeq + 1,
    );
    // Unread source exists, so a resume *would* have produced a plausible board
    // — and then a lineage and snapshot that agree, hiding the skew for good.
    for (const group of commands.slice(3)) await stream.appendBatch(group);
    const before = await readRows(protocol);

    await expect(runRaw(protocol)).rejects.toMatchObject({ _tag: "StateRestorePoison" });
    expect(await readRows(protocol)).toEqual(before);
  });
});
