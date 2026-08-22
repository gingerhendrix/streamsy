#!/usr/bin/env node
/* oxlint-disable effecttsgo/async-function -- This dependency-free Node executable is intentionally Promise-native and bounded at every HTTP, subprocess, and cancellation edge. */
/* oxlint-disable effecttsgo/global-date, effecttsgo/global-fetch, effecttsgo/global-timers, effecttsgo/new-promise, effecttsgo/node-builtin-import, effecttsgo/process-env -- This dependency-free Node launcher directly owns bounded Web requests, subprocesses, cancellation timers, filesystem state, and executable configuration. */

/**
 * Repository-independent controller for one external Streamsy Risk seat.
 *
 * This file intentionally uses only Node built-ins and imports no game or bot
 * code. The launcher owns protocol correctness and process bounds. A selected
 * coding-agent CLI receives a fresh, secret-free observation and chooses one
 * action from legalMoves.
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

/**
 * Command statuses that re-posting cannot change. Authorization (401/403) and
 * throttling (429) are deliberately excluded: those are conditions of the
 * caller, not verdicts on the command, and must never settle an owed ask.
 */
const DETERMINISTIC_REJECTIONS = new Set([400, 404, 409, 410, 422]);

/**
 * The actions resource is a Server-Sent Events stream. The server holds one
 * connection for 30 seconds and then closes it; the client reconnects from the
 * last `nextOffset` it was given, so resume is exact and no position state lives
 * anywhere but that cursor. This bound is the protocol's, not a tunable: the
 * launcher only sizes its own abort timer generously against it.
 */
const ACTIONS_STREAM_TIMEOUT_MS = 30_000;

/**
 * The launcher's own abort timer. It starts before the request is issued, so it
 * runs through connect, TLS, auth and the server's catch-up work; sized at the
 * server's bound exactly it would fire a hair early on every idle connection and
 * discard the closing control frame. The slack keeps the server the party that
 * ends an idle connection, and leaves this timer as the guard for a server that
 * never closes at all. Mirrors `ACTIONS_STREAM_CLIENT_TIMEOUT_MS` in
 * `src/application/actions-stream.ts`; this file stays dependency-free by design.
 */
const ACTIONS_STREAM_CLIENT_TIMEOUT_MS = ACTIONS_STREAM_TIMEOUT_MS + 5_000;

/** @returns {never} */
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
            scalar: { name: "armies", min: legal.pool, max: legal.pool },
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
      case "skip-fortifications":
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
  const resolution = flattenLegalChoices(decision.legalMoves);
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
        action: {
          type: choice.type,
          placements: [{ territoryId: choice.territoryId, armies: selection.armies }],
        },
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
    case "skip-fortifications":
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
    case "reinforce": {
      if (!Array.isArray(action.placements) || action.placements.length === 0) return false;
      const seen = new Set();
      let total = 0;
      for (const placement of action.placements) {
        if (
          !placement ||
          typeof placement !== "object" ||
          Array.isArray(placement) ||
          !legal.territoryIds.includes(placement.territoryId) ||
          seen.has(placement.territoryId) ||
          !Number.isInteger(placement.armies) ||
          placement.armies < 1
        ) {
          return false;
        }
        seen.add(placement.territoryId);
        total += placement.armies;
      }
      return total === (legal.pool ?? legal.maxArmies);
    }
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
    case "skip-fortifications":
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
    decision: `${game}/decision`,
    map: `${game}/map`,
    commands: `${game}/commands`,
    actions: `${game}/players/me/actions`,
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

/**
 * Incremental Server-Sent Events parser.
 *
 * Chunk boundaries fall anywhere, so a partial trailing line is buffered until
 * its newline arrives, and a frame is dispatched only on a blank line. Multiple
 * `data:` lines in one frame are joined with newlines, per the EventSource
 * specification — the actions stream writes a JSON array that way.
 */
function createSseParser() {
  let buffer = "";
  let event = "";
  let data = [];
  return {
    push(chunk) {
      buffer += chunk;
      const frames = [];
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const raw of lines) {
        const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
        if (line === "") {
          if (data.length > 0 || event !== "") {
            frames.push({ event: event || "message", data: data.join("\n") });
          }
          event = "";
          data = [];
          continue;
        }
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        let value = colon === -1 ? "" : line.slice(colon + 1);
        if (value.startsWith(" ")) value = value.slice(1);
        if (field === "event") event = value;
        else if (field === "data") data.push(value);
      }
      return frames;
    },
  };
}

/**
 * Hold one actions connection until it produces messages, terminates, or the
 * server closes it at its own bound. Returns the messages seen and the offset
 * to reconnect from — never an offset ahead of messages this call returned, so
 * a reconnection can neither skip an ask nor replay an answered one.
 */
async function nextActions({ url, headers, signal }) {
  const controller = new AbortController();
  // The server closes first; this only guards a connection that never does.
  const bound = setTimeout(
    () => controller.abort(new Error("actions stream bound")),
    ACTIONS_STREAM_CLIENT_TIMEOUT_MS,
  );
  bound.unref?.();
  const abort = () => controller.abort(signal.reason);
  signal?.addEventListener("abort", abort, { once: true });
  try {
    const response = await fetch(url, {
      headers: { ...headers, accept: "text/event-stream" },
      signal: controller.signal,
    });
    if (response.status !== 200) fail(`actions stream returned HTTP ${response.status}`);
    const contentType = response.headers.get("content-type") ?? "";
    if (!contentType.includes("text/event-stream")) {
      fail(`actions stream returned ${contentType || "no content type"}`);
    }
    const parser = createSseParser();
    const decoder = new TextDecoder();
    const reader = response.body.getReader();
    let messages = [];
    let nextOffset;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        for (const frame of parser.push(decoder.decode(value, { stream: true }))) {
          if (frame.event === "data") {
            messages = messages.concat(JSON.parse(frame.data));
            continue;
          }
          if (frame.event !== "control") continue;
          const control = JSON.parse(frame.data);
          nextOffset = control.nextOffset;
          // A batch is complete at its control event: act on it now rather than
          // holding a connection whose remaining bound buys nothing.
          if (messages.length > 0 || control.closed) return { messages, nextOffset };
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
    }
    return { messages, nextOffset };
  } finally {
    clearTimeout(bound);
    signal?.removeEventListener("abort", abort);
    controller.abort();
  }
}

/**
 * Report a finished seat. Written from two places — the process that observed
 * `GameOver` and any later one that finds the recorded completion — so a restart
 * after the terminal message terminates on the same evidence and the same line
 * of stdout, without needing the message replayed.
 */
async function reportFinished(directory, session, counts, resumed = false) {
  const winner = session.finished.winner;
  await appendEvidence(directory, "finished", {
    ...counts,
    winner,
    ...(resumed ? { resumed: true } : {}),
  });
  process.stdout.write(`${JSON.stringify({ status: "finished", ...counts, winner })}\n`);
}

async function initialize(options) {
  const directory = path.resolve(String(options.state ?? ""));
  if ((!options.seat && !options["seat-file"]) || !options.state)
    fail("init requires --seat '<descriptor JSON>' or --seat-file plus --state");
  await ensureStateDir(directory);

  const seat = JSON.parse(
    options["seat-file"]
      ? await readFile(path.resolve(String(options["seat-file"])), "utf8")
      : String(options.seat),
  );
  const { origin, gameId, playerId, token: capability } = seat;
  if (![origin, gameId, playerId, capability].every((value) => typeof value === "string")) {
    fail("seat descriptor is missing origin, gameId, playerId, or token");
  }
  const privateUrl = new URL(origin);
  if (
    privateUrl.protocol !== "https:" &&
    !["127.0.0.1", "localhost", "::1"].includes(privateUrl.hostname)
  ) {
    fail("plain HTTP is allowed only for loopback origins");
  }

  const session = {
    version: 2,
    origin: privateUrl.origin,
    gameId,
    playerId,
    capability,
    cursor: null,
    discovery: { seatSha256: sha256(JSON.stringify(stable(seat))) },
  };
  await atomicJson(path.join(directory, SESSION_FILE), session);
  await appendEvidence(directory, "initialized", {
    origin: privateUrl.origin,
    descriptorVersion: 2,
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

/**
 * Post one command, retaining the exact bytes until the server has answered.
 *
 * `cursorAfter` is the stream position this command answers *for*. It is
 * committed to the session only once an outcome is in hand, and strictly before
 * the in-flight record is dropped — so every crash window either replays the
 * command (and the server dedupes it) or has already recorded that it is done.
 * The reverse order would let a crash strand a still-required action behind an
 * advanced cursor, which no subsequent server event would ever re-announce.
 */
async function submitStable({
  session,
  directory,
  payload,
  bodyOverride,
  cursorAfter,
  maxPosts,
  requestTimeoutMs,
  retryDelayMs,
  signal,
}) {
  const urls = resourceUrls(session);
  const body = bodyOverride ?? JSON.stringify(payload);
  const bodySha256 = sha256(body);
  const inflightFile = path.join(directory, INFLIGHT_FILE);
  const sessionFile = path.join(directory, SESSION_FILE);
  await atomicJson(inflightFile, { body, bodySha256, cursorAfter: cursorAfter ?? null });
  const settle = async () => {
    if (cursorAfter !== undefined) {
      session.cursor = cursorAfter;
      await atomicJson(sessionFile, session);
    }
    await rm(inflightFile, { force: true });
  };
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
          eventOffsetHash: sha256(String(response.body.eventOffset)),
          bodySha256,
          post: posts,
        });
        await settle();
        return { response: response.body, posts, bodySha256, firstAck };
      }
      // A deterministic rejection is an answer, not a transport failure: the
      // canonical board has already moved past this command (a resolved attack,
      // a passed turn), so re-posting the same bytes can only repeat it. The
      // ask it answers is settled and the cursor may advance — the same reading
      // the in-repo bot applies to its own replays.
      if (DETERMINISTIC_REJECTIONS.has(response.status)) {
        await appendEvidence(directory, "command-conflict", {
          actionType: payload.action.type,
          errorCode: response.body?.error?.code ?? "UNKNOWN",
          httpStatus: response.status,
          bodySha256,
          post: posts,
        });
        await settle();
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
  return fail("post bound reached");
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
  // The long poll it configured no longer exists. Refusing it by name is the
  // only way an operator learns that, rather than silently getting a different
  // blocking behaviour than the one they asked for.
  if (options["wait-ms"] !== undefined) {
    fail("--wait-ms is gone: the actions resource is an SSE stream with a fixed 30s bound");
  }
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
    // A recorded completion is the end of the seat, whatever else is on disk.
    // Reconnecting would resume past a `GameOver` the server will never repeat,
    // on a stream that has nothing further to say — so this process reports the
    // finish it inherited and stops.
    if (session.finished) {
      await reportFinished(directory, session, { commands, decisions, posts }, true);
      return;
    }

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
        // `null` is a legitimate recorded position (the stream's start), so the
        // absent case is distinguished from it rather than merged with it.
        cursorAfter: inflight.cursorAfter === undefined ? undefined : inflight.cursorAfter,
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

    // The map does not exist until `GameStarted`, and a host may well launch the
    // seat before pressing start. 409 is "not yet", so the map is fetched lazily
    // at the first ask — by which point the game is provably running — and the
    // launcher spends the interval waiting on the stream rather than exiting.
    let mapDocument = null;
    const loadMap = async () => {
      for (let attempt = 0; attempt < 5 && !controller.signal.aborted; attempt += 1) {
        if (mapDocument) return mapDocument;
        const response = await fetchJson(urls.map, {}, requestTimeoutMs, controller.signal);
        if (response.status === 200) {
          mapDocument = response.body;
          return mapDocument;
        }
        if (response.status !== 409) fail(`map returned HTTP ${response.status}`);
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
      return fail("map is still unavailable after the game started");
    };

    /** The stream position this process has read to, ahead of the durable cursor. */
    let liveCursor = session.cursor;
    /**
     * The `messageId` of an `ActionRequired` that has been read but not yet
     * answered by a settled command. While one is owed, no later page — not even
     * an empty one — may move the durable cursor, because the durable cursor is
     * the only thing that would ever bring the ask back after a restart.
     */
    let owedAsk = null;
    const commitCursor = async () => {
      if (owedAsk) return;
      session.cursor = liveCursor;
      await atomicJson(path.join(directory, SESSION_FILE), session);
    };

    while (!controller.signal.aborted && decisions < maxDecisions && commands < maxCommands) {
      const actionsUrl = new URL(urls.actions);
      if (liveCursor) actionsUrl.searchParams.set("offset", liveCursor);
      // One SSE connection per iteration: the batch that wakes this loop is
      // followed by a long stretch of thinking and posting, so holding the
      // connection across it would buy nothing and risk a stale socket.
      const stream = await nextActions({
        url: actionsUrl,
        headers: authorizedHeaders(session),
        signal: controller.signal,
      });
      const message = stream.messages.at(-1);
      if (message?.type === "GameOver") {
        // The cursor moving past the terminal message and the completion it
        // records are one fact, so they are persisted in one atomic write.
        // Advancing first and recording afterwards leaves a crash window whose
        // restart is unrecoverable: `GameOver` is never re-announced, and the
        // stream reports `closed` only in the batch that delivered it, so a
        // seat resuming past it would wait forever on a finished game.
        session.cursor = stream.nextOffset ?? liveCursor;
        session.finished = { winner: message.winner };
        await atomicJson(path.join(directory, SESSION_FILE), session);
        await reportFinished(directory, session, { commands, decisions, posts });
        return;
      }
      if (stream.nextOffset !== undefined) liveCursor = stream.nextOffset;
      if (message?.type === "ActionRequired") owedAsk = message.messageId ?? true;
      // Nothing is owed for an empty page, so the cursor is safe to persist. An
      // `ActionRequired` is committed only by its command.
      await commitCursor();
      if (!message) continue;
      if (message.type !== "ActionRequired") continue;
      const observed = {
        player: { id: session.playerId },
        mode: message.mode,
        turn: message.turn,
        board: message.board,
        legalMoves: message.legalMoves,
      };
      decisions += 1;
      await appendEvidence(directory, "action-required", {
        messageId: message.messageId,
        mode: observed.mode,
        legalMoveTypes: observed.legalMoves.map((action) => action.type),
        cursorHash: sha256(String(liveCursor)),
      });
      const map = await loadMap();
      const dynamicById = new Map(message.board.territories.map((row) => [row.id, row]));
      const board = {
        status: "playing",
        phase: message.turn.phase,
        round: message.turn.round,
        activePlayerId: message.turn.activePlayerId,
        reinforcement: message.turn.reinforcement,
        players: message.board.players,
        territories: map.territories.map((territory) => ({
          ...territory,
          adjacentTerritoryIds: territory.neighbours,
          ...dynamicById.get(territory.id),
        })),
        continents: map.continents,
      };
      let modelContract = buildModelContract(observed, board);
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
      // A resolved choice the launcher's own validator refuses means the two
      // readings of the published legal space disagree. Continuing would leave
      // the ask owed forever, so drift is terminal and the cursor stays put:
      // the ask survives, and a fixed process picks it up unchanged.
      if (!actionIsLegal(action, observed.legalMoves)) {
        await appendEvidence(directory, "action-validation-drift", {
          attemptId,
          actionType: action.type,
          terminal: true,
          commandSubmitted: false,
        });
        fail(
          `resolved ${action.type} failed launcher validation against the published legal moves`,
        );
      }

      const payload = {
        commandId: `${harness}-${randomUUID()}`,
        turnId: observed.turn.id,
        action,
      };
      const result = await submitStable({
        session,
        directory,
        payload,
        cursorAfter: liveCursor ?? null,
        maxPosts: maxPostsPerCommand,
        requestTimeoutMs,
        retryDelayMs,
        signal: controller.signal,
      });
      posts += result.posts;
      // Answered either way: `submitStable` has already committed the cursor
      // through this ask before dropping its in-flight record.
      owedAsk = null;
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
      "usage: risk-seat.mjs init (--seat '<json>' | --seat-file <file>) --state <0700-dir> | run --state <dir> --harness <claude|codex> [bounds]",
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
