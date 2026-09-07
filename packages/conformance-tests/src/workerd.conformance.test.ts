/* oxlint-disable typescript/consistent-return -- The cleanup boundary returns Effect-style failure exits while successful branches complete with void. */
import { cpSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe } from "vitest";
import { Miniflare } from "miniflare";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";

const workerPath = resolve(dirname(fileURLToPath(import.meta.url)), "../dist/worker/worker.js");
const config = { baseUrl: "" };
let harness: { readonly miniflare: Miniflare; readonly root: string } | undefined;

const retentionDestination = (root: string): string | undefined => {
  const configured = Bun.env.STREAMSY_WORKERD_RETENTION;
  if (configured === undefined) return undefined;
  if (!isAbsolute(configured)) throw new Error("STREAMSY_WORKERD_RETENTION must be absolute");
  const retention = resolve(configured);
  const destination = resolve(retention, basename(root));
  const sourceToRetention = relative(root, retention);
  const sourceToDestination = relative(root, destination);
  if (
    retention === resolve(root) ||
    (sourceToRetention !== "" &&
      !sourceToRetention.startsWith("..") &&
      !isAbsolute(sourceToRetention)) ||
    (sourceToDestination !== "" &&
      !sourceToDestination.startsWith("..") &&
      !isAbsolute(sourceToDestination))
  ) {
    throw new Error(
      "STREAMSY_WORKERD_RETENTION must not be the persistence root or its descendant",
    );
  }
  return destination;
};

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
  try {
    await current.miniflare?.dispose();
  } catch (error) {
    errors.push(error);
  }
  try {
    const destination = retentionDestination(current.root);
    if (destination !== undefined) {
      mkdirSync(resolve(destination, ".."), { recursive: true });
      cpSync(current.root, destination, { recursive: true });
    }
  } catch (error) {
    errors.push(error);
  }
  try {
    rmSync(current.root, { recursive: true, force: true });
  } catch (error) {
    errors.push(error);
  }
  if (errors.length > 0) throw new AggregateError(errors, "Workerd harness cleanup failed");
};

describe("Effect workerd Cloudflare Durable Object host", () => {
  beforeAll(async () => {
    if (!(await Bun.file(workerPath).exists())) {
      throw new Error("Missing workerd artifact; run bun run build:worker first");
    }
    const root = mkdtempSync(join(tmpdir(), "streamsy-conformance-workerd-"));
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
      harness = undefined;
      const cleanupErrors: Array<unknown> = [];
      try {
        await miniflare?.dispose();
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      try {
        rmSync(root, { recursive: true, force: true });
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
    if (current === undefined) return;
    await cleanupHarness(current);
  });
  runConformanceTests(config);
});
