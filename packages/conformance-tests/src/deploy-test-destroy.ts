#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { deploymentOutputFromJson, workerUrlFrom } from "./deployment-state.ts";

const packageDir = join(import.meta.dirname, "..");
const alchemyEntrypoint = join(packageDir, "alchemy.run.ts");
const appName = "streamsy-conf";
const baseStage = process.env.STAGE || "conformance";
const runId =
  process.env.CONFORMANCE_RUN_ID ||
  `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const stage = process.env.CONFORMANCE_UNIQUE_STAGE === "1" ? `${baseStage}-${runId}` : baseStage;
const workerResourceId = "server";
const testScript = process.env.STREAMSY_DO_TEST_SCRIPT || "test:do:local";
const statePath = join(packageDir, ".alchemy", appName, stage, `${workerResourceId}.json`);

const env = {
  ...process.env,
  STAGE: stage,
};

async function runStep(label: string, command: () => Promise<void>): Promise<void> {
  console.log(`\n==> ${label}`);
  await command();
}

async function runCommand(
  command: string,
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv },
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env,
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
      } else if (signal) {
        reject(new Error(`${command} ${args.join(" ")} exited with signal ${signal}`));
      } else {
        reject(new Error(`${command} ${args.join(" ")} exited with code ${code}`));
      }
    });
  });
}

function readWorkerUrl(): string {
  if (!existsSync(statePath)) {
    throw new Error(
      `Alchemy state file not found at ${statePath}. Was the ${stage} deploy successful?`,
    );
  }

  const output = deploymentOutputFromJson(readFileSync(statePath, "utf8"));

  if (output === null) {
    throw new Error(
      `Alchemy state file ${statePath} did not contain output.url. Ensure the worker has a workers.dev URL enabled.`,
    );
  }

  return workerUrlFrom(output);
}

function logFailure(label: string, error: Error): void {
  console.error(`\n${label}:`);
  console.error(error.message);
}

async function waitForWorkerReady(baseUrl: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError = new Error("Worker readiness check did not receive a response");

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/`);
      const body = await response.text();
      if (response.status === 400 && body.includes("Stream path required")) return;
      lastError = new Error(
        `Unexpected readiness response ${response.status}: ${body.slice(0, 120)}`,
      );
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000));
  }

  throw new Error(`Worker did not become ready at ${baseUrl}: ${String(lastError)}`);
}

let exitCode = 0;

try {
  await runStep(`deploy test server with STAGE=${stage}`, () =>
    runCommand(process.execPath, [alchemyEntrypoint], { cwd: packageDir, env }),
  );

  const serverBaseUrl = readWorkerUrl();
  console.log(`\n==> conformance server: ${serverBaseUrl}`);

  await runStep("wait for deployed worker readiness", () => waitForWorkerReady(serverBaseUrl));

  await runStep(`run ${testScript} against deployed server`, () =>
    runCommand(
      "bun",
      ["run", testScript, ...(testScript === "test:do:local" ? ["--reporter=dot"] : [])],
      {
        cwd: packageDir,
        env: {
          ...env,
          SERVER_BASE_URL: serverBaseUrl,
        },
      },
    ),
  );
} catch (error) {
  exitCode = 1;
  logFailure(
    "Conformance deploy/test failed",
    error instanceof Error ? error : new Error(String(error)),
  );
} finally {
  try {
    await runStep(`destroy test server with STAGE=${stage}`, () =>
      runCommand(process.execPath, [alchemyEntrypoint, "--destroy"], { cwd: packageDir, env }),
    );
  } catch (error) {
    exitCode = 1;
    logFailure(
      "Conformance destroy failed",
      error instanceof Error ? error : new Error(String(error)),
    );
  }
}

process.exit(exitCode);
