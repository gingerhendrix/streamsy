/* oxlint-disable effecttsgo/async-function -- Local workerd lifecycle boundary. */
/**
 * Start the built Worker in local workerd through Miniflare. No Cloudflare
 * account, credentials, or network service is used. Durable Object SQLite state
 * and alarms persist under `directory`, so a second `startWorker` on the same
 * directory recreates every game object from the same storage.
 */
import { Miniflare } from "miniflare";

export interface WorkerOptions {
  /** Listen port. 0 picks a free loopback port. */
  readonly port?: number;
  /** Shorter defence window for tests. Omit for the 15-second game rule. */
  readonly defenseTimeoutMs?: number;
}

export const startWorker = async (directory: string, options: WorkerOptions = {}) => {
  const root = `${import.meta.dirname}/..`;
  const bindings: Record<string, string> = {};
  if (options.defenseTimeoutMs !== undefined) {
    bindings.DEFENSE_TIMEOUT_MS = String(options.defenseTimeoutMs);
  }
  const mf = new Miniflare({
    name: "hex-domination",
    scriptPath: `${root}/dist/worker/worker.js`,
    modules: true,
    compatibilityDate: "2026-08-06",
    compatibilityFlags: ["nodejs_compat"],
    host: "127.0.0.1",
    port: options.port ?? 0,
    cf: false,
    durableObjects: { GAME: { className: "GameDurableObject", useSQLite: true } },
    durableObjectsPersist: `${directory}/objects`,
    bindings,
    assets: {
      directory: `${root}/dist/web`,
      binding: "ASSETS",
      routerConfig: { has_user_worker: true, invoke_user_worker_ahead_of_assets: true },
      assetConfig: { not_found_handling: "single-page-application" },
    },
  });

  let closing: Promise<void> | undefined;
  try {
    return { url: (await mf.ready).origin, stop: () => (closing ??= mf.dispose()) };
  } catch (error) {
    await mf.dispose();
    throw error;
  }
};
