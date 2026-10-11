/* oxlint-disable effecttsgo/async-function -- Local workerd lifecycle boundary, shared by tests and the local executable. */
import { Miniflare } from "miniflare";

/** No Cloudflare account, remote bindings, or provider resources. Only local workerd and SQLite/R2 directories. */
export const startWorker = async (
  directory: string,
  token: string,
  port = 0,
  testHooks = false,
) => {
  const mf = new Miniflare({
    name: "folder-sync",
    scriptPath: `${import.meta.dir}/../public/worker/index.js`,
    modules: true,
    compatibilityDate: "2026-08-06",
    compatibilityFlags: ["nodejs_compat"],
    host: "127.0.0.1",
    port,
    cf: false,
    durableObjects: {
      FOLDERS: { className: "FolderObject", useSQLite: true },
      INDEX: { className: "IndexObject", useSQLite: true },
    },
    durableObjectsPersist: `${directory}/objects`,
    r2Buckets: ["BLOBS"],
    r2Persist: `${directory}/r2`,
    bindings: { WRITER_TOKEN: token, TEST_HOOKS: String(testHooks) },
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
