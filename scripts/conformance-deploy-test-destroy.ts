#!/usr/bin/env bun

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";

const repoRoot = join(import.meta.dirname, "..");
const testServerDir = join(repoRoot, "examples", "test-server");
const appName = "durable-streams";
const stage = "conformance";
const workerResourceId = "durable-streams-server";
const statePath = join(
  testServerDir,
  ".alchemy",
  appName,
  stage,
  `${workerResourceId}.json`,
);

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

let exitCode = 0;

try {
  await runStep(
    `deploy test server with STAGE=${stage}`,
    $`bun alchemy deploy`.cwd(testServerDir).env(env),
  );

  const serverBaseUrl = readWorkerUrl();
  console.log(`\n==> conformance server: ${serverBaseUrl}`);

  await runStep(
    "run Durable Object conformance tests against deployed server",
    $`bun run test --reporter=dot`.cwd(testServerDir).env({
      ...env,
      SERVER_BASE_URL: serverBaseUrl,
    }),
  );
} catch (error) {
  exitCode = 1;
  console.error("\nConformance deploy/test failed:");
  console.error(error);
} finally {
  try {
    await runStep(
      `destroy test server with STAGE=${stage}`,
      $`bun alchemy destroy`.cwd(testServerDir).env(env),
    );
  } catch (error) {
    exitCode = 1;
    console.error("\nConformance destroy failed:");
    console.error(error);
  }
}

process.exit(exitCode);
