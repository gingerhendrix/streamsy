#!/usr/bin/env bun

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";

const packageDir = join(import.meta.dirname, "..");
const appName = "streamsy-conf";
const baseStage = process.env.STAGE || "conformance";
const runId =
  process.env.CONFORMANCE_RUN_ID ||
  `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const stage = process.env.CONFORMANCE_UNIQUE_STAGE === "1" ? `${baseStage}-${runId}` : baseStage;
const workerResourceId = "server";
const statePath = join(packageDir, ".alchemy", appName, stage, `${workerResourceId}.json`);

const env = {
  ...process.env,
  STAGE: stage,
};

async function runStep(label: string, command: Promise<unknown>) {
  console.log(`\n==> ${label}`);
  await command;
}

function readWorkerUrl(): string {
  if (!existsSync(statePath)) {
    throw new Error(
      `Alchemy state file not found at ${statePath}. Was the ${stage} deploy successful?`,
    );
  }

  const state = JSON.parse(readFileSync(statePath, "utf8")) as {
    output?: { url?: string };
  };
  const url = state.output?.url;

  if (!url) {
    throw new Error(
      `Alchemy state file ${statePath} did not contain output.url. Ensure the worker has a workers.dev URL enabled.`,
    );
  }

  return url.replace(/\/$/, "");
}

function logFailure(label: string, error: unknown): void {
  console.error(`\n${label}:`);
  if (error instanceof Error) {
    console.error(error.message);
  } else {
    console.error(error);
  }
}

async function waitForWorkerReady(baseUrl: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/`);
      const body = await response.text();
      if (response.status === 400 && body.includes("Stream path required")) return;
      lastError = new Error(
        `Unexpected readiness response ${response.status}: ${body.slice(0, 120)}`,
      );
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000));
  }

  throw new Error(`Worker did not become ready at ${baseUrl}: ${String(lastError)}`);
}

let exitCode = 0;

try {
  await runStep(
    `deploy test server with STAGE=${stage}`,
    $`bun alchemy deploy`.cwd(packageDir).env(env),
  );

  const serverBaseUrl = readWorkerUrl();
  console.log(`\n==> conformance server: ${serverBaseUrl}`);

  await runStep("wait for deployed worker readiness", waitForWorkerReady(serverBaseUrl));

  await runStep(
    "run Durable Object conformance tests against deployed server",
    $`bun run test:do:local --reporter=dot`.cwd(packageDir).env({
      ...env,
      SERVER_BASE_URL: serverBaseUrl,
    }),
  );
} catch (error) {
  exitCode = 1;
  logFailure("Conformance deploy/test failed", error);
} finally {
  try {
    await runStep(
      `destroy test server with STAGE=${stage}`,
      $`bun alchemy destroy`.cwd(packageDir).env(env),
    );
  } catch (error) {
    exitCode = 1;
    logFailure("Conformance destroy failed", error);
  }
}

process.exit(exitCode);
