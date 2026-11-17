import { foldkit } from "@foldkit/vite-plugin";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const api =
    loadEnv(mode, import.meta.dirname, "DURABLE_RESPONSE_")["DURABLE_RESPONSE_API"] ??
    "http://localhost:1340";

  const proxy = { "/chat": api, "/responses": api, "/debug": api };

  return {
    plugins: [foldkit()],
    optimizeDeps: { entries: ["src/entry.ts"] },
    server: { port: 1342, strictPort: true, proxy },
    preview: { port: 1342, strictPort: true, proxy },
  };
});
