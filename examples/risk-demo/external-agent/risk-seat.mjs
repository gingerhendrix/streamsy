#!/usr/bin/env node

/**
 * Repository-independent controller for one external Streamsy Risk seat.
 *
 * This file intentionally uses only Node built-ins and imports no game or bot
 * code. The launcher owns protocol correctness and process bounds. A selected
 * coding-agent CLI receives a fresh, secret-free observation and chooses one
 * action from legalActions.
 */

import { createHash, randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import process from "node:process";
import path from "node:path";

const SESSION_FILE = "session.json";
const EVIDENCE_FILE = "evidence.jsonl";
const INFLIGHT_FILE = "inflight.json";
const ATTEMPT_SEQUENCE_FILE = "attempt-sequence.json";
const ATTEMPTS_DIRECTORY = "model-attempts";

function fail(message, code = 1) {
  const error = new Error(message);
  error.exitCode = code;
  throw error;
}

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const options = {};
  for (let index = 0; index < rest.length; index += 1) {
    const item = rest[index];
    if (!item.startsWith("--")) fail(`unexpected argument: ${item}`);
    const key = item.slice(2);
    const value = rest[index + 1];
    if (value === undefined || value.startsWith("--")) options[key] = true;
    else {
      options[key] = value;
      index += 1;
    }
  }
  return { command, options };
}

function positiveInteger(value, name, fallback) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) fail(`${name} must be a positive integer`);
  return parsed;
}

async function atomicJson(file, value, mode = 0o600) {
  const temporary = `${file}.${process.pid}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode });
  await chmod(temporary, mode);
  await rename(temporary, file);
}

async function ensureStateDir(directory) {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
}

async function readSession(directory) {
  const file = path.join(directory, SESSION_FILE);
  const session = JSON.parse(await readFile(file, "utf8"));
  if (
    typeof session.origin !== "string" ||
    typeof session.gameId !== "string" ||
    typeof session.playerId !== "string" ||
    typeof session.capability !== "string"
  ) {
    fail("invalid session state");
  }
  return session;
}

async function appendEvidence(directory, kind, fields = {}) {
  const safe = {
    at: new Date().toISOString(),
    kind,
    ...fields,
  };
  const file = path.join(directory, EVIDENCE_FILE);
  await writeFile(file, `${JSON.stringify(safe)}\n`, { flag: "a", mode: 0o600 });
  await chmod(file, 0o600);
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .toSorted(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, stable(child)]),
    );
  }
  return value;
}

function decisionFingerprint(decision) {
  return sha256(JSON.stringify(stable(decision)));
}

function samePrimitive(actual, expected) {
  return typeof actual === typeof expected && actual === expected;
}

function indexValues(values) {
  return new Map(
    [...new Set(values.filter((value) => typeof value === "string"))].map((id, i) => [id, i]),
  );
}

function flattenLegalChoices(legalActions) {
  const choices = [];
  for (const legal of legalActions) {
    switch (legal.type) {
      case "reinforce":
        for (const territoryId of legal.territoryIds) {
          choices.push({
            type: legal.type,
            territoryId,
            scalar: { name: "armies", min: legal.minArmies, max: legal.maxArmies },
          });
        }
        break;
      case "attack":
      case "declare-attack":
        for (const choice of legal.choices) {
          choices.push({
            type: legal.type,
            from: choice.from,
            to: choice.to,
            scalar: { name: "attackerDice", min: 1, max: choice.maxAttackerDice },
          });
        }
        break;
      case "roll-defense":
        choices.push({ type: legal.type, attackId: legal.attackId });
        break;
      case "occupy-territory":
        choices.push({
          type: legal.type,
          attackId: legal.attackId,
          from: legal.from,
          to: legal.to,
          scalar: { name: "armies", min: legal.minArmies, max: legal.maxArmies },
        });
        break;
      case "fortify":
        for (const choice of legal.choices) {
          if (Array.isArray(choice.reachable)) {
            for (const reachable of choice.reachable) {
              choices.push({
                type: legal.type,
                from: choice.from,
                to: reachable.to,
                scalar: { name: "armies", min: 1, max: reachable.maxArmies },
              });
            }
          } else {
            choices.push({
              type: legal.type,
              from: choice.from,
              to: choice.to,
              scalar: { name: "armies", min: 1, max: choice.maxArmies },
            });
          }
        }
        break;
      case "end-turn":
        choices.push({ type: legal.type });
        break;
    }
  }
  return choices;
}

/**
 * Build a compact model-facing view. Canonical identifiers remain in the
 * launcher-only resolution table; the model sees stable indexes for this
 * decision and only the scalar bounds it must choose within.
 */
export function buildModelContract(decision, board) {
  const resolution = flattenLegalChoices(decision.legalActions);
  const territoryIds = [
    ...(board.territories ?? []).map((territory) => territory.id),
    ...resolution.flatMap((choice) => [choice.territoryId, choice.from, choice.to]),
  ];
  const playerIds = [
    ...(board.players ?? []).map((player) => player.id),
    decision.player?.id,
    decision.turn?.activePlayerId,
  ];
  const continentIds = [
    ...(board.continents ?? []).map((continent) => continent.id),
    ...(board.territories ?? []).map((territory) => territory.continentId),
  ];
  const territories = indexValues(territoryIds);
  const players = indexValues(playerIds);
  const continents = indexValues(continentIds);
  const modelReinforcement = (reinforcement) =>
    reinforcement && typeof reinforcement === "object"
      ? {
          base: reinforcement.base,
          continents: (reinforcement.continents ?? []).map((bonus) => ({
            continentIndex: continents.get(bonus.continentId),
            bonus: bonus.bonus,
          })),
          total: reinforcement.total,
          remaining: reinforcement.remaining,
        }
      : undefined;
  const selectionChoices = resolution.map((choice, choiceIndex) => ({
    choiceIndex,
    type: choice.type,
    ...(choice.territoryId === undefined
      ? {}
      : { territoryIndex: territories.get(choice.territoryId) }),
    ...(choice.from === undefined ? {} : { fromTerritoryIndex: territories.get(choice.from) }),
    ...(choice.to === undefined ? {} : { toTerritoryIndex: territories.get(choice.to) }),
    ...(choice.scalar === undefined
      ? {}
      : { [choice.scalar.name]: { min: choice.scalar.min, max: choice.scalar.max } }),
  }));
  return {
    observation: {
      mode: decision.mode,
      turn: {
        round: decision.turn?.round,
        phase: decision.turn?.phase,
        reinforcement: modelReinforcement(decision.turn?.reinforcement),
        activePlayerIndex: players.get(decision.turn?.activePlayerId),
      },
      selfPlayerIndex: players.get(decision.player?.id),
      board: {
        status: board.status,
        phase: board.phase,
        round: board.round,
        activePlayerIndex: players.get(board.activePlayerId),
        players: (board.players ?? []).map((player) => ({
          playerIndex: players.get(player.id),
          controller: player.controller,
          eliminated: player.eliminated,
          ...(player.remainingArmies === undefined
            ? {}
            : { remainingArmies: player.remainingArmies }),
        })),
        territories: (board.territories ?? []).map((territory) => ({
          territoryIndex: territories.get(territory.id),
          ownerPlayerIndex: players.get(territory.ownerId),
          armies: territory.armies,
          continentIndex: continents.get(territory.continentId),
          ...(Array.isArray(territory.adjacentTerritoryIds)
            ? {
                adjacentTerritoryIndexes: territory.adjacentTerritoryIds.map((id) =>
                  territories.get(id),
                ),
              }
            : {}),
        })),
        continents: (board.continents ?? []).map((continent) => ({
          continentIndex: continents.get(continent.id),
          territoryIndexes: continent.territoryIds.map((id) => territories.get(id)),
          reinforcementBonus: continent.reinforcementBonus,
          controllerPlayerIndex: players.get(continent.controllerId),
        })),
        reinforcement: modelReinforcement(board.reinforcement),
      },
      legalChoices: selectionChoices,
    },
    resolution,
  };
}

function exactKeys(value, expected) {
  return (
    Object.keys(value).length === expected.length &&
    expected.every((key) => Object.hasOwn(value, key))
  );
}

/** Resolve a model selection without accepting or reproducing canonical IDs. */
export function resolveModelSelection(selection, resolution) {
  if (!selection || typeof selection !== "object" || Array.isArray(selection)) {
    return { ok: false, reason: "CHOICE_NOT_OBJECT" };
  }
  if (!Number.isInteger(selection.choiceIndex)) {
    return { ok: false, reason: "CHOICE_INDEX_INVALID" };
  }
  const choice = resolution[selection.choiceIndex];
  if (!choice) return { ok: false, reason: "CHOICE_INDEX_UNKNOWN" };
  const scalarName = choice.scalar?.name;
  const expectedKeys = scalarName ? ["choiceIndex", scalarName] : ["choiceIndex"];
  if (!exactKeys(selection, expectedKeys)) {
    return { ok: false, reason: "CHOICE_FIELDS_INVALID" };
  }
  if (scalarName && !Number.isInteger(selection[scalarName])) {
    return { ok: false, reason: "SCALAR_INVALID" };
  }
  if (
    scalarName &&
    (selection[scalarName] < choice.scalar.min || selection[scalarName] > choice.scalar.max)
  ) {
    return { ok: false, reason: "SCALAR_OUT_OF_BOUNDS" };
  }
  switch (choice.type) {
    case "reinforce":
      return {
        ok: true,
        action: { type: choice.type, territoryId: choice.territoryId, armies: selection.armies },
      };
    case "attack":
    case "declare-attack":
      return {
        ok: true,
        action: {
          type: choice.type,
          from: choice.from,
          to: choice.to,
          attackerDice: selection.attackerDice,
        },
      };
    case "roll-defense":
      return { ok: true, action: { type: choice.type, attackId: choice.attackId } };
    case "occupy-territory":
      return {
        ok: true,
        action: { type: choice.type, attackId: choice.attackId, armies: selection.armies },
      };
    case "fortify":
      return {
        ok: true,
        action: {
          type: choice.type,
          from: choice.from,
          to: choice.to,
          armies: selection.armies,
        },
      };
    case "end-turn":
      return { ok: true, action: { type: choice.type } };
    default:
      return { ok: false, reason: "CHOICE_TYPE_UNSUPPORTED" };
  }
}

/** Validate transport shape against the server-published legal action space. */
export function actionIsLegal(action, legalActions) {
  if (!action || typeof action !== "object" || Array.isArray(action)) return false;
  const legal = legalActions.find((candidate) => candidate.type === action.type);
  if (!legal) return false;
  switch (action.type) {
    case "reinforce":
      return (
        legal.territoryIds.includes(action.territoryId) &&
        Number.isInteger(action.armies) &&
        action.armies >= legal.minArmies &&
        action.armies <= legal.maxArmies
      );
    case "attack":
    case "declare-attack": {
      const choice = legal.choices.find(
        (candidate) => candidate.from === action.from && candidate.to === action.to,
      );
      return (
        !!choice &&
        Number.isInteger(action.attackerDice) &&
        action.attackerDice >= 1 &&
        action.attackerDice <= choice.maxAttackerDice
      );
    }
    case "roll-defense":
      return samePrimitive(action.attackId, legal.attackId);
    case "occupy-territory":
      return (
        samePrimitive(action.attackId, legal.attackId) &&
        Number.isInteger(action.armies) &&
        action.armies >= legal.minArmies &&
        action.armies <= legal.maxArmies
      );
    case "fortify":
      if (Array.isArray(legal.choices) && legal.choices[0]?.reachable) {
        const from = legal.choices.find((candidate) => candidate.from === action.from);
        const reachable = from?.reachable.find((candidate) => candidate.to === action.to);
        return (
          !!reachable &&
          Number.isInteger(action.armies) &&
          action.armies >= 1 &&
          action.armies <= reachable.maxArmies
        );
      }
      return legal.choices.some(
        (candidate) =>
          candidate.from === action.from &&
          candidate.to === action.to &&
          Number.isInteger(action.armies) &&
          action.armies >= 1 &&
          action.armies <= candidate.maxArmies,
      );
    case "end-turn":
      return Object.keys(action).length === 1;
    default:
      return false;
  }
}

function authorizedHeaders(session) {
  return { authorization: `Bearer ${session.capability}` };
}

function resourceUrls(session) {
  const game = `${session.origin}/v1/games/${encodeURIComponent(session.gameId)}`;
  return {
    metadata: game,
    decision: `${game}/decision`,
    board: `${game}/board`,
    commands: `${game}/commands`,
    turns: `${game}/players/me/turns`,
  };
}

async function fetchJson(url, options = {}, timeoutMs = 10_000, externalSignal) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error("request timeout")), timeoutMs);
  timeout.unref?.();
  const abort = () => controller.abort(externalSignal.reason);
  externalSignal?.addEventListener("abort", abort, { once: true });
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const text = await response.text();
    let body;
    try {
      body = text === "" ? null : JSON.parse(text);
    } catch {
      fail(`non-JSON response from ${new URL(url).pathname}`);
    }
    return { status: response.status, body };
  } finally {
    clearTimeout(timeout);
    externalSignal?.removeEventListener("abort", abort);
  }
}

async function initialize(options) {
  const seatUrl = String(options["seat-url"] ?? "");
  const directory = path.resolve(String(options.state ?? ""));
  if (!seatUrl || !options.state) fail("init requires --seat-url and --state");
  await ensureStateDir(directory);

  const privateUrl = new URL(seatUrl);
  const fragment = new URLSearchParams(privateUrl.hash.slice(1));
  const capability = fragment.get("token");
  if (!capability) fail("private seat URL has no #token capability");
  privateUrl.hash = "";
  if (privateUrl.search) fail("private seat URL must not contain a query string");
  if (
    privateUrl.protocol !== "https:" &&
    !["127.0.0.1", "localhost", "::1"].includes(privateUrl.hostname)
  ) {
    fail("plain HTTP is allowed only for loopback origins");
  }

  const bootstrapResponse = await fetch(privateUrl, { redirect: "error" });
  if (!bootstrapResponse.ok) fail(`bootstrap returned HTTP ${bootstrapResponse.status}`);
  const bootstrap = await bootstrapResponse.text();
  const origin = bootstrap.match(/^API origin: (.+)$/m)?.[1];
  const gameId = bootstrap.match(/^Game ID: (.+)$/m)?.[1];
  const playerId = bootstrap.match(/^Player ID: (.+)$/m)?.[1];
  const openApi = bootstrap.match(/^OpenAPI: (.+)$/m)?.[1];
  if (!origin || !gameId || !playerId || !openApi)
    fail("bootstrap is missing required discovery fields");
  if (
    new URL(origin).origin !== privateUrl.origin ||
    new URL(openApi).origin !== privateUrl.origin
  ) {
    fail("bootstrap attempted to change origin");
  }
  const openApiResponse = await fetch(openApi, { redirect: "error" });
  if (!openApiResponse.ok) fail(`OpenAPI returned HTTP ${openApiResponse.status}`);
  const openApiDocument = await openApiResponse.json();
  if (openApiDocument.openapi !== "3.1.0") fail("unsupported OpenAPI document");
  for (const requiredPath of [
    "/v1/games/{gameId}",
    "/v1/games/{gameId}/decision",
    "/v1/games/{gameId}/commands",
    "/v1/games/{gameId}/board",
    "/v1/games/{gameId}/players/me/turns",
  ]) {
    if (!openApiDocument.paths?.[requiredPath]) fail(`OpenAPI is missing ${requiredPath}`);
  }

  const session = {
    version: 1,
    origin: privateUrl.origin,
    gameId,
    playerId,
    capability,
    cursor: null,
    discovery: {
      bootstrapSha256: sha256(bootstrap),
      openApiSha256: sha256(JSON.stringify(openApiDocument)),
    },
  };
  await atomicJson(path.join(directory, SESSION_FILE), session);
  await appendEvidence(directory, "initialized", {
    origin: privateUrl.origin,
    bootstrapStatus: bootstrapResponse.status,
    openApiStatus: openApiResponse.status,
  });
  process.stdout.write(`${JSON.stringify({ status: "initialized", origin: privateUrl.origin })}\n`);
}

function strategyPrompt(contract, correctionReason) {
  return `You are choosing exactly one strategic action for a Streamsy Risk seat.
Return only JSON matching {"selectionJson":"<one JSON selection object encoded as a string>"}.
For example: {"selectionJson":"{\\"choiceIndex\\":0,\\"armies\\":2}"}. Do not use tools, inspect files, or discuss the choice.
Choose one listed legalChoices choiceIndex. Include only choiceIndex and the bounded scalar named by that choice, if any.
Mandatory occupation and out-of-turn defence are actionable.
${correctionReason ? `Your previous selection was rejected with ${correctionReason}. Correct it once using this unchanged fresh contract.\n` : ""}
Model contract:
${JSON.stringify(contract)}`;
}

function firstJsonObject(text) {
  let start = -1;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    if (start < 0) {
      if (character === "{") {
        start = index;
        depth = 1;
      }
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') inString = true;
    else if (character === "{") depth += 1;
    else if (character === "}" && --depth === 0) return text.slice(start, index + 1);
  }
  return null;
}

async function runChild(command, args, { cwd, timeoutMs, outputFile, signal }) {
  const child = spawn(command, args, {
    cwd,
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
    detached: false,
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += chunk));
  child.stderr.on("data", (chunk) => (stderr += chunk));
  const kill = () => child.kill("SIGTERM");
  signal.addEventListener("abort", kill, { once: true });
  const timer = setTimeout(() => child.kill("SIGTERM"), timeoutMs);
  timer.unref?.();
  const result = await new Promise((resolve) =>
    child.once("close", (code, childSignal) => resolve({ code, signal: childSignal })),
  );
  clearTimeout(timer);
  signal.removeEventListener("abort", kill);
  if (outputFile) {
    try {
      stdout = await readFile(outputFile, "utf8");
    } catch {
      // Codex may fail before creating its last-message file.
    }
  }
  return { ...result, stdout, stderr, timedOut: result.signal === "SIGTERM" && !signal.aborted };
}

async function nextAttemptId(directory) {
  const file = path.join(directory, ATTEMPT_SEQUENCE_FILE);
  let previous = 0;
  try {
    const stored = JSON.parse(await readFile(file, "utf8"));
    if (!Number.isSafeInteger(stored.lastAttemptId) || stored.lastAttemptId < 0) {
      fail("invalid model attempt sequence");
    }
    previous = stored.lastAttemptId;
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const attemptId = previous + 1;
  await atomicJson(file, { lastAttemptId: attemptId });
  return attemptId;
}

async function chooseSelection(harness, prompt, directory, timeoutMs, budget, signal, attemptId) {
  const modelDirectory = path.join(
    directory,
    ATTEMPTS_DIRECTORY,
    String(attemptId).padStart(6, "0"),
  );
  const sessionFile = path.join(directory, SESSION_FILE);
  await ensureStateDir(modelDirectory);
  const outputFile = path.join(modelDirectory, "last-message.json");
  const schema = {
    type: "object",
    properties: {
      selectionJson: { type: "string" },
    },
    required: ["selectionJson"],
    additionalProperties: false,
  };
  const schemaFile = path.join(modelDirectory, "choice-schema.json");
  await atomicJson(schemaFile, schema);
  await rm(outputFile, { force: true });
  let result;
  // The child never needs capability state. Mode 000 makes that file unreadable
  // even if a model ignores the no-tools prompt; the parent restores it in all
  // ordinary success, failure, timeout, and cancellation paths.
  await chmod(sessionFile, 0o000);
  try {
    if (harness === "claude") {
      result = await runChild(
        optionsExecutable("RISK_CLAUDE_BIN", "claude"),
        [
          "-p",
          prompt,
          "--output-format",
          "text",
          "--json-schema",
          JSON.stringify(schema),
          "--permission-mode",
          "dontAsk",
          "--tools",
          "",
          "--safe-mode",
          "--no-session-persistence",
          "--max-budget-usd",
          String(budget),
        ],
        { cwd: modelDirectory, timeoutMs, signal },
      );
    } else if (harness === "codex") {
      result = await runChild(
        optionsExecutable("RISK_CODEX_BIN", "codex"),
        [
          "-a",
          "never",
          "exec",
          "--ephemeral",
          "--ignore-rules",
          "--skip-git-repo-check",
          "-s",
          "read-only",
          "-C",
          modelDirectory,
          "-o",
          outputFile,
          "--output-schema",
          schemaFile,
          prompt,
        ],
        { cwd: modelDirectory, timeoutMs, outputFile, signal },
      );
    } else fail("--harness must be claude or codex");
  } finally {
    await chmod(sessionFile, 0o600);
  }
  if (harness === "claude") {
    await writeFile(outputFile, result.stdout, { mode: 0o600 });
  }
  await chmod(outputFile, 0o600).catch(() => {});
  if (signal.aborted) fail("cancelled", 130);
  if (result.timedOut) fail(`${harness} strategy subprocess timed out`);
  if (result.code !== 0) fail(`${harness} strategy subprocess exited ${result.code}`);
  const json = firstJsonObject(result.stdout);
  if (!json) return { failureCode: "MODEL_OUTPUT_NOT_JSON" };
  try {
    let choice = JSON.parse(json);
    for (let depth = 0; depth < 3 && typeof choice?.selectionJson === "string"; depth += 1) {
      choice = JSON.parse(choice.selectionJson);
    }
    return { selection: choice?.selection ?? choice };
  } catch {
    return { failureCode: "MODEL_SELECTION_JSON_INVALID" };
  }
}

function optionsExecutable(environmentName, fallback) {
  return process.env[environmentName] || fallback;
}

async function submitStable({
  session,
  directory,
  payload,
  bodyOverride,
  maxPosts,
  requestTimeoutMs,
  retryDelayMs,
  signal,
}) {
  const urls = resourceUrls(session);
  const body = bodyOverride ?? JSON.stringify(payload);
  const bodySha256 = sha256(body);
  await atomicJson(path.join(directory, INFLIGHT_FILE), { body, bodySha256 });
  let posts = 0;
  let firstAck = null;
  while (posts < maxPosts) {
    posts += 1;
    try {
      const response = await fetchJson(
        urls.commands,
        {
          method: "POST",
          headers: { ...authorizedHeaders(session), "content-type": "application/json" },
          body,
        },
        requestTimeoutMs,
        signal,
      );
      if (response.status === 200) {
        if (!["accepted", "duplicate"].includes(response.body?.status)) fail("invalid command ack");
        firstAck ??= response.body;
        await appendEvidence(directory, "command-ack", {
          actionType: payload.action.type,
          ackStatus: response.body.status,
          sourceOffsetHash: sha256(String(response.body.sourceOffset)),
          bodySha256,
          post: posts,
        });
        await rm(path.join(directory, INFLIGHT_FILE), { force: true });
        return { response: response.body, posts, bodySha256, firstAck };
      }
      if (response.status === 409) {
        await appendEvidence(directory, "command-conflict", {
          actionType: payload.action.type,
          errorCode: response.body?.error?.code ?? "UNKNOWN",
          bodySha256,
          post: posts,
        });
        await rm(path.join(directory, INFLIGHT_FILE), { force: true });
        return { conflict: response.body, posts, bodySha256 };
      }
      fail(`command returned HTTP ${response.status}`);
    } catch (error) {
      if (signal.aborted) fail("cancelled", 130);
      await appendEvidence(directory, "transport-retry", {
        actionType: payload.action.type,
        bodySha256,
        post: posts,
      });
      if (posts >= maxPosts) throw error;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    }
  }
  fail("post bound reached");
}

async function runLoop(options) {
  const directory = path.resolve(String(options.state ?? ""));
  if (!options.state) fail("run requires --state");
  const harness = String(options.harness ?? "");
  const maxCommands = positiveInteger(options["max-commands"], "--max-commands", 1);
  const maxPostsPerCommand = positiveInteger(
    options["max-posts-per-command"],
    "--max-posts-per-command",
    2,
  );
  const maxDecisions = positiveInteger(options["max-decisions"], "--max-decisions", 8);
  const wallMs = positiveInteger(options["wall-ms"], "--wall-ms", 120_000);
  const waitMs = positiveInteger(options["wait-ms"], "--wait-ms", 3_000);
  const modelTimeoutMs = positiveInteger(options["model-timeout-ms"], "--model-timeout-ms", 45_000);
  const requestTimeoutMs = positiveInteger(
    options["request-timeout-ms"],
    "--request-timeout-ms",
    10_000,
  );
  const retryDelayMs = positiveInteger(options["retry-delay-ms"], "--retry-delay-ms", 100);
  const budget = Number(options["max-budget-usd"] ?? "1");
  if (!(budget > 0)) fail("--max-budget-usd must be positive");
  const cancelFile = options["cancel-file"] ? path.resolve(String(options["cancel-file"])) : null;

  const controller = new AbortController();
  const cancel = () => controller.abort(new Error("cancelled"));
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  const deadline = setTimeout(() => controller.abort(new Error("wall-time bound")), wallMs);
  deadline.unref?.();
  const watcher = cancelFile
    ? setInterval(async () => {
        try {
          await stat(cancelFile);
          cancel();
        } catch {}
      }, 50)
    : null;
  watcher?.unref?.();

  const session = await readSession(directory);
  const urls = resourceUrls(session);
  let commands = 0;
  let decisions = 0;
  let posts = 0;
  try {
    try {
      const inflight = JSON.parse(await readFile(path.join(directory, INFLIGHT_FILE), "utf8"));
      if (typeof inflight.body !== "string" || sha256(inflight.body) !== inflight.bodySha256) {
        fail("invalid in-flight command state");
      }
      const payload = JSON.parse(inflight.body);
      const resumed = await submitStable({
        session,
        directory,
        payload,
        bodyOverride: inflight.body,
        maxPosts: maxPostsPerCommand,
        requestTimeoutMs,
        retryDelayMs,
        signal: controller.signal,
      });
      posts += resumed.posts;
      if (resumed.response) commands += 1;
      await appendEvidence(directory, "inflight-resumed", {
        actionType: payload.action.type,
        outcome: resumed.response?.status ?? resumed.conflict?.error?.code ?? "unknown",
        bodySha256: inflight.bodySha256,
      });
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }

    while (!controller.signal.aborted && decisions < maxDecisions && commands < maxCommands) {
      const metadata = await fetchJson(urls.metadata, {}, requestTimeoutMs, controller.signal);
      if (metadata.status !== 200) fail(`metadata returned HTTP ${metadata.status}`);
      if (metadata.body.status === "finished") {
        await appendEvidence(directory, "finished", { commands, decisions, posts });
        process.stdout.write(
          `${JSON.stringify({ status: "finished", commands, decisions, posts })}\n`,
        );
        return;
      }

      const turnsUrl = new URL(urls.turns);
      if (session.cursor) turnsUrl.searchParams.set("offset", session.cursor);
      turnsUrl.searchParams.set("wait", String(waitMs));
      const turns = await fetchJson(
        turnsUrl,
        { headers: authorizedHeaders(session) },
        waitMs + requestTimeoutMs,
        controller.signal,
      );
      if (turns.status !== 200) fail(`turn stream returned HTTP ${turns.status}`);
      session.cursor = turns.body.cursor;
      await atomicJson(path.join(directory, SESSION_FILE), session);

      const observed = await fetchJson(
        urls.decision,
        { headers: authorizedHeaders(session) },
        requestTimeoutMs,
        controller.signal,
      );
      if (observed.status !== 200) fail(`decision returned HTTP ${observed.status}`);
      decisions += 1;
      await appendEvidence(directory, "fresh-decision", {
        mode: observed.body.mode,
        legalActionTypes: observed.body.legalActions.map((action) => action.type),
        wakeCount: turns.body.notifications.length,
        cursorHash: sha256(String(session.cursor)),
      });
      if (observed.body.legalActions.length === 0) continue;

      const board = await fetchJson(urls.board, {}, requestTimeoutMs, controller.signal);
      if (board.status !== 200) fail(`board returned HTTP ${board.status}`);
      const fingerprint = decisionFingerprint(observed.body);
      let modelContract = buildModelContract(observed.body, board.body);
      let attemptId = await nextAttemptId(directory);
      await appendEvidence(directory, "model-attempt-started", { attemptId, corrective: false });
      let modelResult = await chooseSelection(
        harness,
        strategyPrompt(modelContract.observation),
        directory,
        modelTimeoutMs,
        budget,
        controller.signal,
        attemptId,
      );
      let resolved = modelResult.failureCode
        ? { ok: false, reason: modelResult.failureCode }
        : resolveModelSelection(modelResult.selection, modelContract.resolution);
      if (!resolved.ok) {
        await appendEvidence(directory, "model-choice-rejected", {
          attemptId,
          corrective: false,
          failureCode: resolved.reason,
          commandSubmitted: false,
        });
        const correctionDecision = await fetchJson(
          urls.decision,
          { headers: authorizedHeaders(session) },
          requestTimeoutMs,
          controller.signal,
        );
        if (correctionDecision.status !== 200) {
          fail(`correction decision returned HTTP ${correctionDecision.status}`);
        }
        if (decisionFingerprint(correctionDecision.body) !== fingerprint) {
          await appendEvidence(directory, "stale-choice-discarded", {
            attemptId,
            failureCode: "DECISION_CHANGED_BEFORE_CORRECTION",
            commandSubmitted: false,
          });
          continue;
        }
        if (decisions >= maxDecisions) {
          await appendEvidence(directory, "model-correction-skipped", {
            attemptId,
            failureCode: "CORRECTION_DECISION_BOUND",
            terminal: true,
            commandSubmitted: false,
          });
          fail(`${harness} model correction would exceed the decision bound`);
        }
        decisions += 1;
        modelContract = buildModelContract(correctionDecision.body, board.body);
        attemptId = await nextAttemptId(directory);
        await appendEvidence(directory, "model-attempt-started", {
          attemptId,
          corrective: true,
          correctionForFailureCode: resolved.reason,
        });
        modelResult = await chooseSelection(
          harness,
          strategyPrompt(modelContract.observation, resolved.reason),
          directory,
          modelTimeoutMs,
          budget,
          controller.signal,
          attemptId,
        );
        resolved = modelResult.failureCode
          ? { ok: false, reason: modelResult.failureCode }
          : resolveModelSelection(modelResult.selection, modelContract.resolution);
        if (!resolved.ok) {
          await appendEvidence(directory, "model-choice-rejected", {
            attemptId,
            corrective: true,
            failureCode: resolved.reason,
            terminal: true,
            commandSubmitted: false,
          });
          fail(`${harness} model choice failed bounded correction: ${resolved.reason}`);
        }
      }
      const action = resolved.action;

      const fresh = await fetchJson(
        urls.decision,
        { headers: authorizedHeaders(session) },
        requestTimeoutMs,
        controller.signal,
      );
      if (fresh.status !== 200) fail(`freshness decision returned HTTP ${fresh.status}`);
      if (decisionFingerprint(fresh.body) !== fingerprint) {
        await appendEvidence(directory, "stale-choice-discarded", {
          attemptId,
          actionType: action.type,
          commandSubmitted: false,
        });
        continue;
      }
      if (!actionIsLegal(action, fresh.body.legalActions)) continue;

      const payload = {
        commandId: `${harness}-${randomUUID()}`,
        turnId: fresh.body.turn.id,
        action,
      };
      const result = await submitStable({
        session,
        directory,
        payload,
        maxPosts: maxPostsPerCommand,
        requestTimeoutMs,
        retryDelayMs,
        signal: controller.signal,
      });
      posts += result.posts;
      if (result.response) commands += 1;
      if (result.conflict) continue;
    }
    const status = controller.signal.aborted ? "cancelled" : "bound-reached";
    await appendEvidence(directory, status, { commands, decisions, posts });
    process.stdout.write(`${JSON.stringify({ status, commands, decisions, posts })}\n`);
    if (controller.signal.aborted) process.exitCode = 130;
  } catch (error) {
    if (!controller.signal.aborted) throw error;
    await appendEvidence(directory, "cancelled", { commands, decisions, posts });
    process.stdout.write(
      `${JSON.stringify({ status: "cancelled", commands, decisions, posts })}\n`,
    );
    process.exitCode = 130;
  } finally {
    clearTimeout(deadline);
    if (watcher) clearInterval(watcher);
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}

async function main() {
  const { command, options } = parseArgs(process.argv.slice(2));
  if (command === "init") await initialize(options);
  else if (command === "run") await runLoop(options);
  else {
    fail(
      "usage: risk-seat.mjs init --seat-url <private-url> --state <0700-dir> | run --state <dir> --harness <claude|codex> [bounds]",
    );
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    const code = Number.isInteger(error.exitCode) ? error.exitCode : 1;
    process.stderr.write(`${error.message}\n`);
    process.exitCode = code;
  });
}
