/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/global-fetch -- This executable example owns the local Miniflare, temporary I/O, and HTTP boundaries. */
import { mkdtemp, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const sourceDirectory = dirname(fileURLToPath(import.meta.url));
const root = resolve(sourceDirectory, "../../../..");
const entrypoint = resolve(root, "packages/serve/test/support/cloudflare/example-worker.ts");

export interface ExampleInstance {
  readonly ready: Promise<{ readonly origin: string }>;
  readonly dispose: () => Promise<void>;
}

export interface ExampleResponse {
  readonly status: number;
  readonly arrayBuffer: () => Promise<ArrayBuffer>;
}

export interface ExampleLifecycle {
  readonly build: (outputRoot: string) => Promise<string>;
  readonly start: (workerPath: string, statePath: string) => ExampleInstance;
  readonly request: (origin: string) => Promise<ExampleResponse>;
  readonly removeRoot?: (path: string) => Promise<void>;
  readonly disposalTimeoutMs?: number;
  readonly onRoot?: (path: string) => void;
}

const withTimeout = async <A>(promise: Promise<A>, timeoutMs: number): Promise<A> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<A>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`Example disposal exceeded ${timeoutMs} ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
};

const aggregate = (primary: Error | undefined, cleanup: ReadonlyArray<Error>): never => {
  const errors = primary === undefined ? [...cleanup] : [primary, ...cleanup];
  if (errors.length === 1) throw errors[0];
  throw new AggregateError(errors, "Cloudflare example failed and cleanup was incomplete", {
    cause: primary ?? errors[0],
  });
};

export const runOwnedExample = async (lifecycle: ExampleLifecycle): Promise<void> => {
  const ownedRoot = await mkdtemp(join(tmpdir(), ".streamsy-cloudflare-example-"));
  let instance: ExampleInstance | undefined;
  let primaryError: Error | undefined;
  const cleanupErrors: Error[] = [];

  try {
    lifecycle.onRoot?.(ownedRoot);
    const workerPath = await lifecycle.build(join(ownedRoot, "worker"));
    instance = lifecycle.start(workerPath, join(ownedRoot, "state"));
    const { origin } = await instance.ready;
    const response = await lifecycle.request(origin);
    await response.arrayBuffer();
    if (response.status !== 201) {
      throw new Error(`Cloudflare example create failed: ${response.status}`);
    }
  } catch (error) {
    primaryError = error instanceof Error ? error : new Error(String(error));
  } finally {
    let disposed = instance === undefined;
    if (instance !== undefined) {
      const disposalErrors: Error[] = [];
      const timeoutMs = lifecycle.disposalTimeoutMs ?? 1_000;
      for (let attempt = 0; attempt < 2 && !disposed; attempt++) {
        try {
          await withTimeout(instance.dispose(), timeoutMs);
          disposed = true;
        } catch (error) {
          disposalErrors.push(error instanceof Error ? error : new Error(String(error)));
        }
      }
      if (!disposed) {
        cleanupErrors.push(
          ...disposalErrors,
          new Error(`Cloudflare example instance remains owned; retained root: ${ownedRoot}`),
        );
      }
    }
    if (disposed) {
      try {
        await (lifecycle.removeRoot?.(ownedRoot) ??
          rm(ownedRoot, { recursive: true, force: true }));
      } catch (error) {
        cleanupErrors.push(
          error instanceof Error ? error : new Error(String(error)),
          new Error(`Cloudflare example root retained: ${ownedRoot}`),
        );
      }
    }
  }

  if (primaryError !== undefined || cleanupErrors.length > 0)
    aggregate(primaryError, cleanupErrors);
};

const lifecycle: ExampleLifecycle = {
  build: async (outputRoot) => {
    const build = await Bun.build({
      entrypoints: [entrypoint],
      outdir: outputRoot,
      target: "browser",
      format: "esm",
      minify: false,
      sourcemap: "none",
      external: ["cloudflare:workers"],
    });
    if (!build.success) throw new Error("Cloudflare example Worker build failed");
    const worker = build.outputs.find((output) => output.path.endsWith(".js"));
    if (worker === undefined) throw new Error("Cloudflare example Worker emitted no JavaScript");
    return worker.path;
  },
  start: (workerPath, statePath) =>
    new Miniflare({
      scriptPath: workerPath,
      modules: true,
      modulesRoot: "/",
      compatibilityDate: "2026-07-30",
      compatibilityFlags: ["nodejs_compat"],
      host: "127.0.0.1",
      port: 0,
      cf: false,
      durableObjects: { STREAMS: { className: "StreamsObject", useSQLite: true } },
      durableObjectsPersist: statePath,
    }),
  request: (origin) =>
    fetch(new URL("/streams/events", origin), {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    }),
};

export const runExample = (): Promise<void> => runOwnedExample(lifecycle);

if (import.meta.main) await runExample();
