/* oxlint-disable typescript/consistent-return -- The cleanup boundary returns Effect-style failure exits while successful branches complete with void. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe } from "vitest";
import { Miniflare } from "miniflare";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { reclaimRoot } from "./workerd-harness.ts";

const workerPath = resolve(dirname(fileURLToPath(import.meta.url)), "../dist/worker/worker.js");
const config = { baseUrl: "" };
let harness: { readonly miniflare: Miniflare; readonly root: string } | undefined;
const ownedRoots = new Set<string>();

const cleanupHarness = async (current: {
  readonly miniflare?: Miniflare;
  readonly root: string;
}): Promise<void> => {
  const errors: Array<unknown> = [];
  try {
    if (current.miniflare !== undefined) {
      const ids = await current.miniflare.listDurableObjectIds("STREAMS");
      process.stderr.write(
        `workerd conformance profile=single-object-chain instantiated-objects=${ids.length} ids=${ids.join(",")}\n`,
      );
      if (ids.length !== 1)
        throw new Error(`Expected one instantiated Durable Object, got ${ids.length}`);
    }
  } catch (error) {
    errors.push(error);
  }
  let disposed = current.miniflare === undefined;
  try {
    await current.miniflare?.dispose();
    disposed = true;
  } catch (error) {
    errors.push(error);
  }
  if (disposed) {
    const rootErrors = reclaimRoot(current.root, Bun.env.STREAMSY_WORKERD_RETENTION);
    errors.push(...rootErrors);
    if (rootErrors.length === 0) ownedRoots.delete(current.root);
  } else {
    errors.push(new Error("Persistence root retained while Miniflare disposal is unresolved"));
  }
  if (errors.length > 0) throw new AggregateError(errors, "Workerd harness cleanup failed");
};

describe("Effect workerd Cloudflare Durable Object host", () => {
  beforeAll(async () => {
    if (!(await Bun.file(workerPath).exists())) {
      throw new Error("Missing workerd artifact; run bun run build:worker first");
    }
    const root = mkdtempSync(join(tmpdir(), "streamsy-conformance-workerd-"));
    ownedRoots.add(root);
    let miniflare: Miniflare | undefined;
    try {
      miniflare = new Miniflare({
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
      harness = { miniflare, root };
      config.baseUrl = (await miniflare.ready).origin;
    } catch (error) {
      const cleanupErrors: Array<unknown> = [];
      try {
        await miniflare?.dispose();
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      try {
        rmSync(root, { recursive: true, force: true });
        ownedRoots.delete(root);
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
    const current = harness;
    harness = undefined;
    const errors: Array<unknown> = [];
    if (current !== undefined) {
      try {
        await cleanupHarness(current);
      } catch (error) {
        errors.push(error);
        if (ownedRoots.has(current.root)) {
          try {
            await cleanupHarness(current);
          } catch (retryError) {
            errors.push(retryError);
          }
        }
      }
    }
    for (const root of Array.from(ownedRoots)) {
      const rootErrors = reclaimRoot(root, Bun.env.STREAMSY_WORKERD_RETENTION);
      errors.push(...rootErrors);
      if (rootErrors.length === 0) ownedRoots.delete(root);
    }
    if (errors.length > 0) throw new AggregateError(errors, "Workerd afterAll cleanup failed");
  });
  runConformanceTests(config);
});
