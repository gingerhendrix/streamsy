/* oxlint-disable typescript/consistent-return -- The cleanup boundary returns Effect-style failure exits while successful branches complete with void. */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe } from "vitest";
import { Miniflare } from "miniflare";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import {
  cleanupWorkerdState,
  type WorkerdDisposable,
  type WorkerdOwnedState,
} from "./workerd-harness.ts";

const workerPath = resolve(dirname(fileURLToPath(import.meta.url)), "../dist/worker/worker.js");
const config = { baseUrl: "" };
const ownedStates = new Set<WorkerdOwnedState>();

const cleanupHarness = async (current: WorkerdOwnedState): Promise<void> => {
  const errors: Array<unknown> = [];
  try {
    if (current.instance !== undefined && "listDurableObjectIds" in current.instance) {
      // SAFETY: the guard above is the Miniflare namespace inspection method used by this runner.
      const miniflare = current.instance as Miniflare;
      const ids = await miniflare.listDurableObjectIds("STREAMS");
      process.stderr.write(
        `workerd conformance profile=single-object-chain instantiated-objects=${ids.length} ids=${ids.join(",")}\n`,
      );
      if (ids.length !== 1)
        throw new Error(`Expected one instantiated Durable Object, got ${ids.length}`);
    }
  } catch (error) {
    errors.push(error);
  }
  const cleanup = await cleanupWorkerdState(current, Bun.env.STREAMSY_WORKERD_RETENTION);
  errors.push(...cleanup.errors);
  if (cleanup.done) ownedStates.delete(current);
  if (errors.length > 0) throw new AggregateError(errors, "Workerd harness cleanup failed");
};

describe("Effect workerd Cloudflare Durable Object host", () => {
  beforeAll(async () => {
    if (!(await Bun.file(workerPath).exists())) {
      throw new Error("Missing workerd artifact; run bun run build:worker first");
    }
    const root = mkdtempSync(join(tmpdir(), "streamsy-conformance-workerd-"));
    const state: WorkerdOwnedState<WorkerdDisposable & Miniflare> = {
      root,
      disposed: false,
    };
    ownedStates.add(state);
    try {
      const miniflare = new Miniflare({
        scriptPath: workerPath,
        modules: true,
        compatibilityDate: "2026-07-30",
        compatibilityFlags: ["nodejs_compat"],
        host: "127.0.0.1",
        port: 0,
        cf: false,
        durableObjects: { STREAMS: { className: "StreamsObject", useSQLite: true } },
        durableObjectsPersist: join(root, "state"),
      });
      state.instance = miniflare;
      config.baseUrl = (await miniflare.ready).origin;
    } catch (error) {
      const cleanupErrors: Array<unknown> = [];
      try {
        const cleanup = await cleanupWorkerdState(state, Bun.env.STREAMSY_WORKERD_RETENTION);
        cleanupErrors.push(...cleanup.errors);
        if (cleanup.done) ownedStates.delete(state);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      if (cleanupErrors.length > 0) {
        // oxlint-disable-next-line eslint(preserve-caught-error) -- AggregateError retains the primary startup error and every cleanup error.
        throw new AggregateError([error, ...cleanupErrors], "Workerd startup failed", {
          cause: error,
        });
      }
      throw error;
    }
  });
  afterAll(async () => {
    const errors: Array<unknown> = [];
    for (const current of Array.from(ownedStates)) {
      try {
        await cleanupHarness(current);
      } catch (error) {
        errors.push(error);
        if (ownedStates.has(current)) {
          try {
            await cleanupHarness(current);
          } catch (retryError) {
            errors.push(retryError);
          }
        }
      }
    }
    if (errors.length > 0) throw new AggregateError(errors, "Workerd afterAll cleanup failed");
  });
  runConformanceTests(config);
});
