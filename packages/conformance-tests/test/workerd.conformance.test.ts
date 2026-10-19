import { Effect } from "effect";
/* oxlint-disable typescript/consistent-return -- The cleanup boundary returns Effect-style failure exits while successful branches complete with void. */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe } from "vitest";
import { Miniflare } from "miniflare";
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { createWorkerdRunnerLifecycle, type WorkerdDisposable } from "../src/workerd-harness.ts";

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
  ready: (miniflare) => miniflare.ready.then((url) => url.origin),
  inspect: (miniflare) =>
    miniflare.listDurableObjectIds("STREAMS").then((ids) => {
      process.stderr.write(
        `workerd conformance profile=single-object-chain instantiated-objects=${ids.length} ids=${ids.join(",")}\n`,
      );

      if (ids.length !== 1)
        throw new Error(`Expected one instantiated Durable Object, got ${ids.length}`);
    }),
  configuredRetention: Bun.env.STREAMSY_WORKERD_RETENTION,
});

describe("Effect workerd Cloudflare Durable Object host", () => {
  beforeAll(() =>
    Effect.runPromise(
      Effect.gen(function* () {
        if (!(yield* Effect.promise(() => Bun.file(workerPath).exists()))) {
          throw new Error("Missing workerd artifact; run bun run build:worker first");
        }

        config.baseUrl = yield* Effect.promise(() => lifecycle.beforeAll());
      }),
    ),
  );
  afterAll(() => lifecycle.afterAll());
  runConformanceTests(config);
});
