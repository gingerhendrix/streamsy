import { foldkit } from "@foldkit/vite-plugin";
import { defineConfig, loadEnv } from "vite";

// The base app's server answers `/api` and `/streams`. Start it with
// `bun run dev:api`. Set ISSUE_TRACKER_API to use another server.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, import.meta.dirname, "ISSUE_TRACKER_");
  const api = env["ISSUE_TRACKER_API"] ?? "http://localhost:1339";
  const proxy = { "/api": api, "/streams": api };

  return {
    plugins: [foldkit()],
    optimizeDeps: { entries: ["src/entry.ts"] },
    server: { port: 1338, strictPort: true, proxy },
    preview: { port: 1338, strictPort: true, proxy },
  };
});
