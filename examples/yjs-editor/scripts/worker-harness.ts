/* oxlint-disable effecttsgo/async-function -- Local workerd lifecycle boundary. */
import { Miniflare } from "miniflare";

export const startWorker = async (
  directory: string,
  port = 0,
  compactionThreshold = 1024 * 1024,
) => {
  const mf = new Miniflare({
    name: "yjs-editor",
    scriptPath: `${import.meta.dir}/../public/worker/index.js`,
    modules: true,
    compatibilityDate: "2026-08-06",
    compatibilityFlags: ["nodejs_compat"],
    host: "127.0.0.1",
    port,
    cf: false,
    durableObjects: { DOCUMENTS: { className: "DocumentObject", useSQLite: true } },
    durableObjectsPersist: `${directory}/objects`,
    bindings: { COMPACTION_THRESHOLD: String(compactionThreshold) },
    assets: {
      directory: `${import.meta.dir}/../public/web`,
      binding: "ASSETS",
      routerConfig: { has_user_worker: true, invoke_user_worker_ahead_of_assets: true },
      assetConfig: { not_found_handling: "single-page-application" },
    },
  });

  let closing: Promise<void> | undefined;

  try {
    return { mf, url: (await mf.ready).origin, stop: () => (closing ??= mf.dispose()) };
  } catch (error) {
    await mf.dispose();
    throw error;
  }
};
