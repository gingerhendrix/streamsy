/**
 * The repository-independent launcher, exercised as a process against a fixture
 * server that speaks the four-endpoint agent contract.
 *
 * The pure helpers (`buildModelContract`, `resolveModelSelection`,
 * `actionIsLegal`) are unit-tested here too, but the properties that matter are
 * only observable by actually running `runLoop`: that it waits rather than dies
 * before the host presses start, that a lost response is retried with the same
 * bytes, that an out-of-bounds model choice costs one correction and never an
 * illegal POST, that cancellation is prompt and leaves evidence, and — the one
 * this suite exists for — that a process killed between reading an ask and
 * acknowledging its command resumes and finishes it instead of deadlocking.
 */

import { execFile, spawn } from "node:child_process";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";

import { afterEach, describe, expect, it } from "vitest";

import {
  actionIsLegal,
  buildModelContract,
  resolveModelSelection,
} from "../../external-agent/risk-seat.mjs";

const execFileAsync = promisify(execFile);
const LAUNCHER = path.resolve("external-agent/risk-seat.mjs");
const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixtureRoot(): Promise<string> {
  const root = await mkdtemp(path.join(process.env.RISK_AGENT_TEST_TMP ?? tmpdir(), "risk-seat-"));
  await chmod(root, 0o700);
  roots.push(root);
  return root;
}

// ---------------------------------------------------------------------------
// Fixture server: the four agent endpoints, and nothing else.
// ---------------------------------------------------------------------------

const MAP = {
  gameId: "game",
  territories: [
    { id: "a", name: "Alpha", neighbours: ["b"], continentId: "c1" },
    { id: "b", name: "Beta", neighbours: ["a"], continentId: "c1" },
  ],
  continents: [{ id: "c1", name: "Cee", territoryIds: ["a", "b"], reinforcementBonus: 2 }],
};

/** One mandatory occupation: exactly one legal move, with a bounded scalar. */
function occupationAsk(seq: number) {
  return {
    type: "ActionRequired",
    messageId: `act:game:player:${seq}`,
    seq,
    gameId: "game",
    playerId: "player",
    reason: "occupation-required",
    turn: {
      id: "turn-1",
      round: 1,
      phase: "attack",
      activePlayerId: "player",
      reinforcement: { base: 3, continents: [], total: 3, remaining: 0 },
    },
    mode: "active-turn",
    pendingInteraction: { type: "occupation", attackId: "attack", from: "a", to: "b" },
    legalMoves: [
      {
        type: "occupy-territory",
        attackId: "attack",
        from: "a",
        to: "b",
        minArmies: 2,
        maxArmies: 4,
        submit: { type: "occupy-territory", attackId: "attack", armies: "<2..4>" },
      },
    ],
    board: {
      territories: [
        { id: "a", ownerId: "player", armies: 5 },
        { id: "b", ownerId: null, armies: 0 },
      ],
      players: [{ id: "player", eliminated: false }],
    },
    since: { fromEventOffset: null, events: [] },
    eventOffset: `off-${seq}`,
  };
}

interface FixtureState {
  /** Before the host presses start there is no map: `/map` answers 409. */
  started: boolean;
  messages: unknown[];
  commandBodies: string[];
  /** Destroy the socket on the first POST, simulating a lost response. */
  dropFirstResponse: boolean;
  /** Record the command but never answer it, simulating a crash mid-flight. */
  hangCommand: boolean;
  parked: ServerResponse[];
  mapReads: number;
}

function fixtureState(overrides: Partial<FixtureState> = {}): FixtureState {
  return {
    started: true,
    messages: [occupationAsk(1)],
    commandBodies: [],
    dropFirstResponse: false,
    hangCommand: false,
    parked: [],
    mapReads: 0,
    ...overrides,
  };
}

function sendJson(response: ServerResponse, body: unknown, status = 200): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

async function readBody(request: IncomingMessage): Promise<string> {
  let body = "";
  for await (const chunk of request) body += chunk;
  return body;
}

async function startFixture(state: FixtureState) {
  const server = createServer(async (request, response) => {
    const url = new URL(request.url!, "http://127.0.0.1");

    if (url.pathname === "/v1/games/game/map") {
      state.mapReads += 1;
      if (!state.started) {
        return sendJson(
          response,
          { status: "rejected", error: { code: "GAME_NOT_STARTED", message: "not yet" } },
          409,
        );
      }
      return sendJson(response, MAP);
    }

    if (url.pathname === "/v1/games/game/players/me/actions") {
      // Offsets are plain indexes into the fixture's message list.
      const from = Number.parseInt(url.searchParams.get("offset") ?? "0", 10);
      const pending = state.messages.slice(from);
      if (pending.length === 0) {
        const wait = Number.parseInt(url.searchParams.get("wait") ?? "0", 10);
        // A long wait is parked, so an external cancellation has something real
        // to abort. A short one returns promptly, so the loop keeps spinning.
        if (wait >= 5_000) {
          state.parked.push(response);
          return;
        }
        return sendJson(response, { messages: [], nextOffset: String(from), upToDate: true });
      }
      return sendJson(response, {
        messages: pending,
        nextOffset: String(state.messages.length),
        upToDate: true,
      });
    }

    if (url.pathname === "/v1/games/game/commands") {
      const body = await readBody(request);
      const seen = state.commandBodies.includes(body);
      state.commandBodies.push(body);
      if (state.dropFirstResponse && state.commandBodies.length === 1) {
        request.socket.destroy();
        return;
      }
      if (state.hangCommand) {
        state.parked.push(response);
        return;
      }
      return sendJson(response, {
        status: seen ? "duplicate" : "accepted",
        commandId: JSON.parse(body).commandId,
        turnId: JSON.parse(body).turnId,
        eventOffset: "off-1",
      });
    }

    response.writeHead(404).end();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("fixture did not bind");
  return {
    origin: `http://127.0.0.1:${address.port}`,
    close: async () => {
      for (const parked of state.parked) parked.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

/**
 * A stand-in coding-agent CLI. It reads the prompt off argv, proves the seat
 * file is unreadable from the child, and answers with the first legal choice at
 * its minimum scalar — or a deliberately out-of-bounds one when asked to.
 */
async function fakeHarness(root: string): Promise<string> {
  const file = path.join(root, "fake-harness.mjs");
  await writeFile(
    file,
    `#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
const args = process.argv.slice(2);
if (process.env.FAKE_EXIT) process.exit(Number(process.env.FAKE_EXIT));
const prompt = args.find((arg) => arg.includes("Model contract:"));
if (!prompt) process.exit(2);
try {
  await readFile("../../session.json", "utf8");
  process.exit(3);
} catch {}
const contract = JSON.parse(prompt.split("Model contract:\\n")[1]);
const legal = contract.legalChoices[0];
const corrective = prompt.includes("Your previous selection was rejected");
let selection = { choiceIndex: legal?.choiceIndex ?? 0 };
if (legal?.armies) selection.armies = legal.armies.min;
if (legal?.attackerDice) selection.attackerDice = legal.attackerDice.min;
if (process.env.FAKE_ALWAYS_INVALID || (process.env.FAKE_INVALID_FIRST && !corrective)) {
  selection = { choiceIndex: legal?.choiceIndex ?? 0, armies: 999 };
}
const output = JSON.stringify({ selectionJson: JSON.stringify(selection) });
const outputIndex = args.indexOf("-o");
if (outputIndex >= 0) await writeFile(args[outputIndex + 1], output);
else process.stdout.write(output + "\\n");
`,
    { mode: 0o700 },
  );
  await chmod(file, 0o700);
  return file;
}

async function initialize(root: string, origin: string): Promise<string> {
  const state = path.join(root, "state");
  const seat = {
    origin,
    gameId: "game",
    playerId: "player",
    token: "rsk_test",
    urls: {
      map: "/v1/games/game/map",
      actions: "/v1/games/game/players/me/actions",
      decision: "/v1/games/game/decision",
      commands: "/v1/games/game/commands",
    },
  };
  await execFileAsync("node", [LAUNCHER, "init", "--seat", JSON.stringify(seat), "--state", state]);
  return state;
}

function run(
  state: string,
  harness: "claude" | "codex",
  binary: string,
  extra: string[] = [],
  env: NodeJS.ProcessEnv = {},
) {
  return execFileAsync(
    "node",
    [
      LAUNCHER,
      "run",
      "--state",
      state,
      "--harness",
      harness,
      "--max-commands",
      "1",
      "--max-decisions",
      "2",
      "--max-posts-per-command",
      "2",
      "--wall-ms",
      "8000",
      "--wait-ms",
      "10",
      "--model-timeout-ms",
      "4000",
      "--request-timeout-ms",
      "1000",
      "--retry-delay-ms",
      "20",
      ...extra,
    ],
    { env: { ...process.env, RISK_CLAUDE_BIN: binary, RISK_CODEX_BIN: binary, ...env } },
  );
}

const readJson = async (file: string) => JSON.parse(await readFile(file, "utf8"));
const evidenceOf = (state: string) => readFile(path.join(state, "evidence.jsonl"), "utf8");

// ---------------------------------------------------------------------------

describe("repository-independent external-seat launcher", () => {
  it("validates the published reinforcement pool and submit shape", () => {
    const legalMoves = [
      {
        type: "reinforce",
        territoryIds: ["a", "b"],
        pool: 4,
        submit: { type: "reinforce", placements: [] },
      },
    ];
    expect(
      actionIsLegal(
        {
          type: "reinforce",
          placements: [
            { territoryId: "a", armies: 1 },
            { territoryId: "b", armies: 3 },
          ],
        },
        legalMoves,
      ),
    ).toBe(true);
    // Short of the pool, and the same territory twice, are both refused before
    // any request leaves the process.
    expect(
      actionIsLegal(
        { type: "reinforce", placements: [{ territoryId: "a", armies: 3 }] },
        legalMoves,
      ),
    ).toBe(false);
    expect(
      actionIsLegal(
        {
          type: "reinforce",
          placements: [
            { territoryId: "a", armies: 2 },
            { territoryId: "a", armies: 2 },
          ],
        },
        legalMoves,
      ),
    ).toBe(false);
  });

  it("keeps canonical ids in the launcher and resolves an indexed model choice", () => {
    const decision = {
      player: { id: "p1" },
      mode: "active-turn",
      turn: { id: "turn", phase: "attack", activePlayerId: "p1" },
      legalMoves: [
        {
          type: "occupy-territory",
          attackId: "secret-attack",
          from: "secret-a",
          to: "secret-b",
          minArmies: 2,
          maxArmies: 4,
        },
      ],
    };
    const contract = buildModelContract(decision, {
      players: [{ id: "p1", eliminated: false }],
      territories: [
        { id: "secret-a", ownerId: "p1", armies: 5 },
        { id: "secret-b", ownerId: null, armies: 0 },
      ],
      continents: [],
    });
    expect(JSON.stringify(contract.observation)).not.toContain("secret-");
    expect(resolveModelSelection({ choiceIndex: 0, armies: 3 }, contract.resolution)).toEqual({
      ok: true,
      action: { type: "occupy-territory", attackId: "secret-attack", armies: 3 },
    });
    expect(resolveModelSelection({ choiceIndex: 0, armies: 5 }, contract.resolution)).toEqual({
      ok: false,
      reason: "SCALAR_OUT_OF_BOUNDS",
    });
  });

  for (const harness of ["claude", "codex"] as const) {
    it(`${harness} profile transports a model-selected mandatory occupation`, async () => {
      const root = await fixtureRoot();
      const state = fixtureState();
      const fixture = await startFixture(state);
      try {
        const session = await initialize(root, fixture.origin);
        const result = await run(session, harness, await fakeHarness(root));
        expect(JSON.parse(result.stdout).status).toBe("bound-reached");
        expect(state.commandBodies).toHaveLength(1);
        expect(JSON.parse(state.commandBodies[0]!).action).toEqual({
          type: "occupy-territory",
          attackId: "attack",
          armies: 2,
        });
        // The command answered the ask, so the cursor may finally move past it.
        expect((await readJson(path.join(session, "session.json"))).cursor).toBe("1");
      } finally {
        await fixture.close();
      }
    });
  }

  it("waits for the map instead of exiting when the host has not started yet", async () => {
    const root = await fixtureRoot();
    const state = fixtureState({ started: false, messages: [] });
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      // A pre-start launcher must spend its wall clock polling, not die on the
      // 409 the immutable map correctly answers before `GameStarted`. Reaching
      // the wall bound is exit 130 (an abort), so the run rejects; what matters
      // is *why* it stopped.
      const result = await run(session, "claude", await fakeHarness(root), [
        "--wall-ms",
        "700",
      ]).catch((error) => error);
      expect(result.code).toBe(130);
      expect(result.stderr).toBe("");
      const summary = JSON.parse(result.stdout);
      expect(summary.status).toBe("cancelled");
      expect(summary.commands).toBe(0);
      expect(summary.decisions).toBe(0);
      // The map is fetched lazily, at the first ask — so a pre-start run never
      // touches it at all.
      expect(state.mapReads).toBe(0);
      expect(await evidenceOf(session)).not.toContain("map returned HTTP");
    } finally {
      await fixture.close();
    }
  });

  it("corrects one out-of-bounds choice without ever POSTing an illegal action", async () => {
    const root = await fixtureRoot();
    const state = fixtureState();
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      await run(session, "claude", await fakeHarness(root), [], { FAKE_INVALID_FIRST: "1" });
      expect(state.commandBodies).toHaveLength(1);
      expect(JSON.parse(state.commandBodies[0]!).action.armies).toBe(2);
      const evidence = await evidenceOf(session);
      expect(evidence).toContain('"failureCode":"SCALAR_OUT_OF_BOUNDS","commandSubmitted":false');
      expect(evidence).toContain('"corrective":true');
      expect(await readJson(path.join(session, "attempt-sequence.json"))).toEqual({
        lastAttemptId: 2,
      });
    } finally {
      await fixture.close();
    }
  });

  it("stops after one failed correction, submitting nothing", async () => {
    const root = await fixtureRoot();
    const state = fixtureState();
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      await expect(
        run(session, "codex", await fakeHarness(root), [], { FAKE_ALWAYS_INVALID: "1" }),
      ).rejects.toMatchObject({
        stderr: expect.stringContaining(
          "codex model choice failed bounded correction: SCALAR_OUT_OF_BOUNDS",
        ),
      });
      expect(state.commandBodies).toHaveLength(0);
      expect(await evidenceOf(session)).toContain('"terminal":true,"commandSubmitted":false');
      // Nothing was answered, so nothing was consumed.
      expect((await readJson(path.join(session, "session.json"))).cursor).toBeNull();
    } finally {
      await fixture.close();
    }
  });

  it("retries a lost response with byte-identical input and takes the duplicate", async () => {
    const root = await fixtureRoot();
    const state = fixtureState({ dropFirstResponse: true });
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      await run(session, "claude", await fakeHarness(root));
      expect(state.commandBodies).toHaveLength(2);
      expect(state.commandBodies[1]).toBe(state.commandBodies[0]);
      const evidence = await evidenceOf(session);
      expect(evidence).toContain('"kind":"transport-retry"');
      expect(evidence).toContain('"ackStatus":"duplicate"');
    } finally {
      await fixture.close();
    }
  });

  it("external cancellation aborts a pending long poll and exits 130", async () => {
    const root = await fixtureRoot();
    const state = fixtureState({ messages: [] });
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      const binary = await fakeHarness(root);
      const cancelFile = path.join(root, "cancel");
      const child = spawn(
        "node",
        [
          LAUNCHER,
          "run",
          "--state",
          session,
          "--harness",
          "claude",
          "--max-commands",
          "1",
          "--max-decisions",
          "1",
          "--wall-ms",
          "8000",
          "--wait-ms",
          "30000",
          "--cancel-file",
          cancelFile,
        ],
        { env: { ...process.env, RISK_CLAUDE_BIN: binary }, stdio: ["ignore", "pipe", "pipe"] },
      );
      await new Promise((resolve) => setTimeout(resolve, 200));
      await writeFile(cancelFile, "");
      const code = await new Promise<number | null>((resolve) => child.once("close", resolve));
      expect(code).toBe(130);
      expect(state.commandBodies).toHaveLength(0);
      expect(await evidenceOf(session)).toContain('"kind":"cancelled"');
    } finally {
      await fixture.close();
    }
  });

  describe("crash recovery", () => {
    it("does not consume an ask it never answered", async () => {
      const root = await fixtureRoot();
      const state = fixtureState();
      const fixture = await startFixture(state);
      try {
        const session = await initialize(root, fixture.origin);
        const binary = await fakeHarness(root);

        // The model subprocess dies after the ask has been read. This is the
        // livelock window: if the cursor advanced at read time, the still-required
        // occupation would be behind it and no future event would re-announce it.
        await expect(run(session, "claude", binary, [], { FAKE_EXIT: "9" })).rejects.toBeTruthy();
        expect(state.commandBodies).toHaveLength(0);
        expect((await readJson(path.join(session, "session.json"))).cursor).toBeNull();

        // A fresh process re-reads the same ask and completes it.
        const result = await run(session, "claude", binary);
        expect(JSON.parse(result.stdout).commands).toBe(1);
        expect(state.commandBodies).toHaveLength(1);
        expect((await readJson(path.join(session, "session.json"))).cursor).toBe("1");
      } finally {
        await fixture.close();
      }
    });

    it("replays an unacknowledged command verbatim and then advances", async () => {
      const root = await fixtureRoot();
      const state = fixtureState({ hangCommand: true });
      const fixture = await startFixture(state);
      try {
        const session = await initialize(root, fixture.origin);
        const binary = await fakeHarness(root);

        // The command reaches the server; the response never comes back. The
        // process gives up with the exact bytes retained on disk.
        await expect(
          run(session, "claude", binary, ["--max-posts-per-command", "1"]),
        ).rejects.toBeTruthy();
        expect(state.commandBodies).toHaveLength(1);
        const inflight = await readJson(path.join(session, "inflight.json"));
        expect(inflight.body).toBe(state.commandBodies[0]);
        expect(inflight.cursorAfter).toBe("1");
        // Crucially, the cursor is still where it was: the ask is not yet answered.
        expect((await readJson(path.join(session, "session.json"))).cursor).toBeNull();

        // Restart against a responsive server: the same bytes go out again and
        // the server recognises them as the command it already holds.
        state.hangCommand = false;
        for (const parked of state.parked.splice(0)) parked.destroy();
        const result = await run(session, "claude", binary);
        expect(state.commandBodies).toHaveLength(2);
        expect(state.commandBodies[1]).toBe(state.commandBodies[0]);
        const evidence = await evidenceOf(session);
        expect(evidence).toContain('"kind":"inflight-resumed"');
        expect(evidence).toContain('"outcome":"duplicate"');
        expect(JSON.parse(result.stdout).status).toBe("bound-reached");
        // The ask is answered exactly once, and the cursor has moved past it.
        expect((await readJson(path.join(session, "session.json"))).cursor).toBe("1");
        await expect(readFile(path.join(session, "inflight.json"), "utf8")).rejects.toMatchObject({
          code: "ENOENT",
        });
      } finally {
        await fixture.close();
      }
    });
  });
});
