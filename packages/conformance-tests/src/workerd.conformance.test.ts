/* oxlint-disable typescript/consistent-return -- The cleanup boundary returns Effect-style failure exits while successful branches complete with void. */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe } from "vitest";
import { Miniflare } from "miniflare";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import {
  createWorkerdRunnerLifecycle,
  type WorkerdDisposable,
} from "./workerd-harness.ts";

const workerPath = resolve(dirname(fileURLToPath(import.meta.url)), "../dist/worker/worker.js");
const config = { baseUrl: "" };
const lifecycle = createWorkerdRunnerLifecycle<WorkerdDisposable & Miniflare>({
  createRoot: () => mkdtempSync(join(tmpdir(), "streamsy-conformance-workerd-")),
  createInstance: (root) =>
    new Miniflare({
      scriptPath: workerPath,
      modules: true,
      compatibilityDate: "2026-07-30",
      compatibilityFlags: ["nodejs_compat"],
      host: "127.0.0.1",
      port: 0,
      cf: false,
      durableObjects: { STREAMS: { className: "StreamsObject", useSQLite: true } },
      durableObjectsPersist: join(root, "state"),
    }),
  ready: async (miniflare) => (await miniflare.ready).origin,
  inspect: async (miniflare) => {
    const ids = await miniflare.listDurableObjectIds("STREAMS");
    process.stderr.write(
      `workerd conformance profile=single-object-chain instantiated-objects=${ids.length} ids=${ids.join(",")}\n`,
    );
    if (ids.length !== 1)
      throw new Error(`Expected one instantiated Durable Object, got ${ids.length}`);
  },
  configuredRetention: Bun.env.STREAMSY_WORKERD_RETENTION,
});

describe("Effect workerd Cloudflare Durable Object host", () => {
  beforeAll(async () => {
    if (!(await Bun.file(workerPath).exists())) {
      throw new Error("Missing workerd artifact; run bun run build:worker first");
    }
    config.baseUrl = await lifecycle.beforeAll();
  });
  afterAll(() => lifecycle.afterAll());
  runConformanceTests(config);
});
