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

interface FixtureState {
  decision: any;
  decisionAfterControl?: any;
  controlled: boolean;
  dropFirstResponse: boolean;
  commandBodies: string[];
  hangingResponses: ServerResponse[];
}

async function fixtureRoot(): Promise<string> {
  const base = process.env.RISK_AGENT_TEST_TMP ?? tmpdir();
  const root = await mkdtemp(path.join(base, "risk-seat-test-"));
  await chmod(root, 0o700);
  roots.push(root);
  return root;
}

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

function sendJson(response: ServerResponse, body: unknown): void {
  response.writeHead(200, { "content-type": "application/json" });
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
    if (url.pathname === "/control") {
      state.controlled = true;
      return sendJson(response, { ok: true });
    }
    if (url.pathname === "/openapi.json") {
      return sendJson(response, {
        openapi: "3.1.0",
        paths: Object.fromEntries(
          [
            "/v1/games/{gameId}",
            "/v1/games/{gameId}/decision",
            "/v1/games/{gameId}/commands",
            "/v1/games/{gameId}/board",
            "/v1/games/{gameId}/players/me/turns",
          ].map((item) => [item, {}]),
        ),
      });
    }
    if (url.pathname === "/agent-seat/game/player") {
      response.writeHead(200, { "content-type": "text/plain" });
      return response.end(`API origin: ${origin}
OpenAPI: ${origin}/openapi.json
Game ID: game
Player ID: player
`);
    }
    if (url.pathname === "/v1/games/game") {
      return sendJson(response, { status: "active" });
    }
    if (url.pathname === "/v1/games/game/board") {
      return sendJson(response, { territories: [] });
    }
    if (url.pathname === "/v1/games/game/decision") {
      return sendJson(
        response,
        state.controlled ? (state.decisionAfterControl ?? state.decision) : state.decision,
      );
    }
    if (url.pathname === "/v1/games/game/players/me/turns") {
      if (url.searchParams.get("wait") === "30000") {
        state.hangingResponses.push(response);
        return;
      }
      return sendJson(response, {
        notifications: [{ type: "TurnAvailable", turnId: "stale-turn" }],
        cursor: "0001",
        upToDate: true,
      });
    }
    if (url.pathname === "/v1/games/game/commands") {
      const body = await readBody(request);
      state.commandBodies.push(body);
      if (state.dropFirstResponse && state.commandBodies.length === 1) {
        request.socket.destroy();
        return;
      }
      return sendJson(response, {
        status: state.commandBodies.length === 1 ? "accepted" : "duplicate",
        commandId: JSON.parse(body).commandId,
        sourceOffset: "canonical-offset",
        events: [],
      });
    }
    response.writeHead(404).end();
  });
  let origin = "";
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("fixture did not bind");
  origin = `http://127.0.0.1:${address.port}`;
  return {
    origin,
    close: async () => {
      for (const response of state.hangingResponses) response.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

async function fakeHarness(root: string): Promise<string> {
  const file = path.join(root, "fake-harness.mjs");
  await writeFile(
    file,
    `#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
const args = process.argv.slice(2);
const prompt = args.find((arg) => arg.includes("Model contract:"));
if (!prompt) process.exit(2);
try {
  await readFile("../../session.json", "utf8");
  process.exit(3);
} catch {}
if (process.env.FAKE_CONTROL_URL) await fetch(process.env.FAKE_CONTROL_URL);
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

function baseDecision(legalActions: any[]) {
  return {
    mode: legalActions[0]?.type === "roll-defense" ? "defense" : "active-turn",
    turn: { id: "turn-1", phase: "attack" },
    board: { sourceThroughOffset: "offset-1" },
    legalActions,
  };
}

async function initialize(root: string, origin: string) {
  const state = path.join(root, "state");
  await execFileAsync("node", [
    LAUNCHER,
    "init",
    "--seat-url",
    `${origin}/agent-seat/game/player#token=test-capability`,
    "--state",
    state,
  ]);
  return state;
}

async function run(
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
      "5000",
      "--wait-ms",
      "10",
      "--model-timeout-ms",
      "2000",
      "--request-timeout-ms",
      "1000",
      ...extra,
    ],
    {
      env: {
        ...process.env,
        RISK_CLAUDE_BIN: binary,
        RISK_CODEX_BIN: binary,
        ...env,
      },
    },
  );
}

describe("repository-independent external-seat launcher", () => {
  it("validates action bounds without choosing strategy", () => {
    const legal = [
      {
        type: "occupy-territory",
        attackId: "attack",
        from: "a",
        to: "b",
        minArmies: 2,
        maxArmies: 4,
      },
    ];
    expect(actionIsLegal({ type: "occupy-territory", attackId: "attack", armies: 2 }, legal)).toBe(
      true,
    );
    expect(actionIsLegal({ type: "occupy-territory", attackId: "attack", armies: 5 }, legal)).toBe(
      false,
    );
  });

  it("uses indexed model choices and resolves opaque identifiers inside the launcher", () => {
    const legalActions = [
      {
        type: "occupy-territory",
        attackId: "opaque-attack",
        from: "opaque-a",
        to: "opaque-b",
        minArmies: 2,
        maxArmies: 4,
      },
    ];
    const contract = buildModelContract(baseDecision(legalActions), {
      territories: [
        { id: "opaque-a", ownerId: "player-a", armies: 5, continentId: "opaque-continent" },
        { id: "opaque-b", ownerId: "player-b", armies: 0, continentId: "opaque-continent" },
      ],
      players: [
        { id: "player-a", controller: "external", eliminated: false },
        { id: "player-b", controller: "human", eliminated: false },
      ],
      reinforcement: {
        base: 3,
        continents: [{ continentId: "opaque-continent", bonus: 2 }],
        total: 5,
        remaining: 5,
      },
    });
    expect(JSON.stringify(contract.observation)).not.toContain("opaque");
    expect(contract.observation.legalChoices).toEqual([
      {
        choiceIndex: 0,
        type: "occupy-territory",
        fromTerritoryIndex: 0,
        toTerritoryIndex: 1,
        armies: { min: 2, max: 4 },
      },
    ]);
    expect(resolveModelSelection({ choiceIndex: 0, armies: 3 }, contract.resolution)).toEqual({
      ok: true,
      action: { type: "occupy-territory", attackId: "opaque-attack", armies: 3 },
    });
  });

  for (const harness of ["claude", "codex"] as const) {
    it(`${harness} profile transports a model-selected mandatory occupation`, async () => {
      const root = await fixtureRoot();
      const state = {
        decision: baseDecision([
          {
            type: "occupy-territory",
            attackId: "attack",
            from: "a",
            to: "b",
            minArmies: 2,
            maxArmies: 4,
          },
        ]),
        controlled: false,
        dropFirstResponse: false,
        commandBodies: [],
        hangingResponses: [],
      };
      const fixture = await startFixture(state);
      try {
        const session = await initialize(root, fixture.origin);
        const binary = await fakeHarness(root);
        const result = await run(session, harness, binary);
        expect(JSON.parse(result.stdout).status).toBe("bound-reached");
        expect(state.commandBodies).toHaveLength(1);
        expect(JSON.parse(state.commandBodies[0]!).action).toEqual({
          type: "occupy-territory",
          attackId: "attack",
          armies: 2,
        });
      } finally {
        await fixture.close();
      }
    });
  }

  it("corrects one invalid mandatory occupation without an illegal POST", async () => {
    const root = await fixtureRoot();
    const state = {
      decision: baseDecision([
        {
          type: "occupy-territory",
          attackId: "attack",
          from: "a",
          to: "b",
          minArmies: 2,
          maxArmies: 4,
        },
      ]),
      controlled: false,
      dropFirstResponse: false,
      commandBodies: [],
      hangingResponses: [],
    };
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      const binary = await fakeHarness(root);
      await run(session, "claude", binary, [], { FAKE_INVALID_FIRST: "1" });
      expect(state.commandBodies).toHaveLength(1);
      expect(JSON.parse(state.commandBodies[0]!).action).toEqual({
        type: "occupy-territory",
        attackId: "attack",
        armies: 2,
      });
      const evidence = await readFile(path.join(session, "evidence.jsonl"), "utf8");
      expect(evidence).toContain(
        '"kind":"model-choice-rejected","attemptId":1,"corrective":false,"failureCode":"SCALAR_OUT_OF_BOUNDS","commandSubmitted":false',
      );
      expect(evidence).toContain(
        '"kind":"model-attempt-started","attemptId":2,"corrective":true,"correctionForFailureCode":"SCALAR_OUT_OF_BOUNDS"',
      );
      const first = await readFile(
        path.join(session, "model-attempts", "000001", "last-message.json"),
        "utf8",
      );
      const second = await readFile(
        path.join(session, "model-attempts", "000002", "last-message.json"),
        "utf8",
      );
      expect(first).toContain('\\"armies\\":999');
      expect(second).toContain('\\"armies\\":2');
      expect(
        JSON.parse(await readFile(path.join(session, "attempt-sequence.json"), "utf8")),
      ).toEqual({ lastAttemptId: 2 });
    } finally {
      await fixture.close();
    }
  });

  it("stops after one failed correction and never POSTs an illegal occupation", async () => {
    const root = await fixtureRoot();
    const state = {
      decision: baseDecision([
        {
          type: "occupy-territory",
          attackId: "attack",
          from: "a",
          to: "b",
          minArmies: 2,
          maxArmies: 4,
        },
      ]),
      controlled: false,
      dropFirstResponse: false,
      commandBodies: [],
      hangingResponses: [],
    };
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      const binary = await fakeHarness(root);
      await expect(
        run(session, "codex", binary, [], { FAKE_ALWAYS_INVALID: "1" }),
      ).rejects.toMatchObject({
        stderr: expect.stringContaining(
          "codex model choice failed bounded correction: SCALAR_OUT_OF_BOUNDS",
        ),
      });
      expect(state.commandBodies).toHaveLength(0);
      const evidence = await readFile(path.join(session, "evidence.jsonl"), "utf8");
      expect(evidence).toContain(
        '"kind":"model-choice-rejected","attemptId":2,"corrective":true,"failureCode":"SCALAR_OUT_OF_BOUNDS","terminal":true,"commandSubmitted":false',
      );
      expect(
        JSON.parse(await readFile(path.join(session, "attempt-sequence.json"), "utf8")),
      ).toEqual({ lastAttemptId: 2 });
    } finally {
      await fixture.close();
    }
  });

  it("retries byte-equivalent command input and accepts the duplicate at one source offset", async () => {
    const root = await fixtureRoot();
    const state = {
      decision: baseDecision([
        {
          type: "occupy-territory",
          attackId: "attack",
          from: "a",
          to: "b",
          minArmies: 1,
          maxArmies: 3,
        },
      ]),
      controlled: false,
      dropFirstResponse: true,
      commandBodies: [],
      hangingResponses: [],
    };
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      const binary = await fakeHarness(root);
      await run(session, "claude", binary);
      expect(state.commandBodies).toHaveLength(2);
      expect(state.commandBodies[1]).toBe(state.commandBodies[0]);
      const evidence = await readFile(path.join(session, "evidence.jsonl"), "utf8");
      expect(evidence).toContain('"kind":"transport-retry"');
      expect(evidence).toContain('"ackStatus":"duplicate"');
    } finally {
      await fixture.close();
    }
  });

  it("replayed stale wake fetches fresh authority and submits no stale command", async () => {
    const root = await fixtureRoot();
    const state = {
      decision: baseDecision([]),
      controlled: false,
      dropFirstResponse: false,
      commandBodies: [],
      hangingResponses: [],
    };
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      const binary = await fakeHarness(root);
      await run(session, "codex", binary, ["--max-decisions", "1"]);
      expect(state.commandBodies).toHaveLength(0);
      const evidence = await readFile(path.join(session, "evidence.jsonl"), "utf8");
      expect(evidence).toContain('"wakeCount":1');
      expect(evidence).toContain('"legalActionTypes":[]');
    } finally {
      await fixture.close();
    }
  });

  it("discards a defence choice when timeout wins the freshness race", async () => {
    const root = await fixtureRoot();
    const state = {
      decision: baseDecision([
        { type: "roll-defense", attackId: "attack", dice: 2, deadlineAt: Date.now() + 1000 },
      ]),
      decisionAfterControl: baseDecision([]),
      controlled: false,
      dropFirstResponse: false,
      commandBodies: [],
      hangingResponses: [],
    };
    const fixture = await startFixture(state);
    try {
      const session = await initialize(root, fixture.origin);
      const binary = await fakeHarness(root);
      await run(session, "claude", binary, [], { FAKE_CONTROL_URL: `${fixture.origin}/control` });
      expect(state.commandBodies).toHaveLength(0);
      const evidence = await readFile(path.join(session, "evidence.jsonl"), "utf8");
      expect(evidence).toContain('"kind":"stale-choice-discarded"');
      expect(evidence).toContain('"commandSubmitted":false');
    } finally {
      await fixture.close();
    }
  });

  it("external cancellation aborts a pending long poll and exits 130", async () => {
    const root = await fixtureRoot();
    const state = {
      decision: baseDecision([]),
      controlled: false,
      dropFirstResponse: false,
      commandBodies: [],
      hangingResponses: [],
    };
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
          "5000",
          "--wait-ms",
          "30000",
          "--cancel-file",
          cancelFile,
        ],
        {
          env: { ...process.env, RISK_CLAUDE_BIN: binary },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      await new Promise((resolve) => setTimeout(resolve, 100));
      await writeFile(cancelFile, "");
      const code = await new Promise<number | null>((resolve) => child.once("close", resolve));
      expect(code).toBe(130);
      expect(state.commandBodies).toHaveLength(0);
      const evidence = await readFile(path.join(session, "evidence.jsonl"), "utf8");
      expect(evidence).toContain('"kind":"cancelled"');
    } finally {
      await fixture.close();
    }
  });
});
