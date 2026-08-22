/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  directProtocolClient,
} from "@streamsy/core";
import { catchUp } from "../board/mesh-test-harness.ts";
import { MESH_LINEAGE_TYPE, MESH_RESERVED_TYPE_PREFIX } from "@streamsy/experimental/ivm-mesh";

import { writeCanonicalEvents } from "../board/board-projection.ts";
import { createBoardMesh, type BoardMaterialized } from "../board/mesh.ts";
import type { GameEvent } from "../domain/events.ts";
import { startGame } from "../../test/testkit.ts";

import {
  boardRowsFromQueries,
  createRiskBoardSession,
  riskBoardState,
  type RiskBoardDb,
} from "./board-stream-db.ts";

const REINFORCEMENT = { base: 5, continents: [], total: 5, remaining: 2 };

describe("Hex Domination StreamDB query shaping", () => {
  it("collapses the zero-or-one turn and combat collections to a single row or null", () => {
    const base = {
      games: [{ id: "g2", status: "playing" as const, round: 1 }],
      players: [],
      hexes: [],
      territories: [],
      continents: [],
      moves: [],
      projectionMeta: [],
    };

    const idle = boardRowsFromQueries({ ...base, turn: [], combat: [] });
    expect(idle?.turn).toBeNull();
    expect(idle?.combat).toBeNull();

    const pending = boardRowsFromQueries({
      ...base,
      turn: [
        {
          id: "turn",
          turnId: "round-1:p1",
          round: 1,
          playerId: "p1",
          phase: "attack",
          reinforcement: REINFORCEMENT,
          reinforcementsPlaced: 3,
          attacksDeclared: 1,
          throwsResolved: 0,
          captures: 0,
          eliminations: 0,
        },
      ],
      combat: [
        {
          id: "combat",
          attackId: "atk-1",
          turnId: "round-1:p1",
          status: "awaiting-defense",
          attackerId: "p1",
          defenderId: "p2",
          from: "t:01",
          to: "t:02",
          attackerDice: 3,
          attackerRolls: [6, 5, 2],
          defenderDice: 2,
          declaredAt: 1_000,
          defenseDeadlineAt: 16_000,
        },
      ],
    });
    expect(pending?.turn?.turnId).toBe("round-1:p1");
    expect(pending?.combat?.status).toBe("awaiting-defense");
  });

  it("keys the current combat collection so a cleared interrupt is a delete", () => {
    expect(riskBoardState.combat.delete({ key: "combat" })).toMatchObject({
      type: "combat",
      key: "combat",
      headers: { operation: "delete" },
    });
  });
});

afterEach(() => vi.unstubAllGlobals());

describe("Risk board session", () => {
  it("encapsulates typed collections and delegates preload and awaitTxId", async () => {
    vi.stubGlobal("window", { location: { origin: "https://risk.test" } });
    const collections = { games: {}, players: {}, territories: {}, moves: {}, projectionMeta: {} };
    const preload = vi.fn(async () => undefined);
    const awaitTxId = vi.fn(async () => undefined);
    const db = {
      collections,
      offset: "42",
      preload,
      close: vi.fn(),
      utils: { awaitTxId },
    } as unknown as RiskBoardDb;
    const createDb = vi.fn((_options: unknown) => db);

    const session = createRiskBoardSession({ streamId: "games/g1/board/board1", createDb });
    await session.preload();
    await session.awaitTxId("risk-board:cmd:42", 1234);

    expect(session.collections).toBe(collections);
    expect(session.offset).toBe("42");
    expect(preload).toHaveBeenCalledOnce();
    expect(awaitTxId).toHaveBeenCalledWith("risk-board:cmd:42", 1234);
    expect(createDb.mock.calls[0]?.[0]).toMatchObject({
      streamOptions: { url: "https://risk.test/streams/games/g1/board/board1" },
      live: "long-poll",
      state: riskBoardState,
    });
  });

  it("closes the owned StreamDB exactly once across concurrent and later calls", async () => {
    vi.stubGlobal("window", { location: { origin: "https://risk.test" } });
    const close = vi.fn();
    const db = {
      collections: {},
      offset: "-1",
      preload: vi.fn(),
      close,
      utils: { awaitTxId: vi.fn() },
    } as unknown as RiskBoardDb;
    const session = createRiskBoardSession({
      streamId: "board",
      createDb: () => db,
    });

    const first = session.close();
    const concurrent = session.close();
    expect(concurrent).toBe(first);
    await Promise.all([first, concurrent]);
    expect(session.close()).toBe(first);
    expect(close).toHaveBeenCalledOnce();
  });
});

/**
 * The mesh writes one reserved `__streamsy.mesh.lineage.v1` row into every board
 * transaction, alongside the application's rows. The browser mirror must handle
 * that row correctly, and "correctly" here means *ignore it*: lineage is the
 * projector's bookkeeping, not board state, and putting it in the UI schema
 * would publish a framework internal as an application collection.
 *
 * That ignoring is not accidental. `@durable-streams/state`'s dispatcher looks
 * the event type up in its handler map and returns when there is none
 * (`dist/db.js`: `const handler = this.handlers.get(event.type); if (!handler)
 * return;`), so an unregistered type is dropped rather than throwing or
 * corrupting a collection. These tests pin both halves of that arrangement: the
 * mirror registers no reserved type, and the projection emits no application row
 * the mirror would silently drop for the same reason.
 */
describe("reserved mesh lineage and the browser mirror", () => {
  const registeredTypes = new Set(
    Object.values(riskBoardState).map((definition) => (definition as { type: string }).type),
  );

  it("registers no reserved __streamsy. collection, so lineage rows are ignored", () => {
    for (const type of registeredTypes) {
      expect(type.startsWith(MESH_RESERVED_TYPE_PREFIX)).toBe(false);
    }
    expect(registeredTypes.has(MESH_LINEAGE_TYPE)).toBe(false);
  });

  it("registers every row type the board projection actually writes", async () => {
    const events = startGame({ players: 2, mapSeed: "mirror-coverage" }).log;
    const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const source = "games/game/events";
    const output = "games/game/projections/board/board1";
    await writeCanonicalEvents(protocol, source, events);
    await protocol.create(output, { contentType: "application/json" });
    const client = directProtocolClient(protocol);
    const mesh = await createBoardMesh({
      gameId: "game",
      client,
      sourceStreamId: source,
      outputStreamId: output,
      generation: "board1",
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
    expect(result.status).toBe("caught-up");

    const stream = await protocol.get(output);
    if (stream.status !== "ok") throw new Error("no board stream");
    const read = await stream.stream.read({});
    if (read.status !== "ok") throw new Error("cannot read board stream");
    const written = read.messages.map(
      (message) => JSON.parse(new TextDecoder().decode(message.data)) as { type: string },
    );

    const reserved = written.filter((row) => row.type.startsWith(MESH_RESERVED_TYPE_PREFIX));
    expect(reserved.length).toBeGreaterThan(0);
    expect(new Set(reserved.map((row) => row.type))).toEqual(new Set([MESH_LINEAGE_TYPE]));

    // Anything the projection writes that is not reserved must have a home in
    // the mirror, or the UI would drop it by the very policy that protects it
    // from the lineage row.
    for (const row of written) {
      if (row.type.startsWith(MESH_RESERVED_TYPE_PREFIX)) continue;
      expect(registeredTypes.has(row.type)).toBe(true);
    }
  });
});
