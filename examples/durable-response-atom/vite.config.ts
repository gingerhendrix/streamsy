import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const api =
    loadEnv(mode, import.meta.dirname, "DURABLE_RESPONSE_")["DURABLE_RESPONSE_API"] ??
    "http://localhost:1340";

  const proxy = { "/chat": api, "/responses": api, "/debug": api };

  return {
    plugins: [react()],
    server: { port: 1341, strictPort: true, proxy },
    preview: { port: 1341, strictPort: true, proxy },
  };
});
