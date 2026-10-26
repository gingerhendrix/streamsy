import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

const folders = Cloudflare.DurableObject("Folders", { className: "FolderObject" });

const index = Cloudflare.DurableObject("Index", { className: "IndexObject" });

export default Alchemy.Stack(
  "folder-sync",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const token = yield* Config.Redacted("FOLDER_SYNC_WRITER_TOKEN");
    // A stage's blobs are disposable demo data. Destroy empties the bucket, then deletes it.
    const blobs = yield* Cloudflare.R2.Bucket("Blobs", { forceDestroy: true });

    const worker = yield* Cloudflare.Worker("Server", {
      name: `folder-sync-${stage}`,
      main: "public/worker/index.js",
      bundle: false,
      compatibility: { date: "2026-08-06", flags: ["nodejs_compat"] },
      env: { FOLDERS: folders, INDEX: index, BLOBS: blobs, WRITER_TOKEN: token },
      assets: {
        directory: "public/web",
        binding: "ASSETS",
        runWorkerFirst: true,
        notFoundHandling: "single-page-application",
      },
    });

    return { url: worker.url };
  }),
);
