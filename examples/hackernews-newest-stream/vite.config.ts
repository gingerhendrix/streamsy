import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiPort = process.env.PORT ?? "1339";
const apiTarget = `http://localhost:${apiPort}`;

export default defineConfig({
  plugins: [react()],
  root: ".",
  server: {
    port: 5175,
    proxy: {
      "/api": apiTarget,
      "/streams": apiTarget,
    },
  },
  resolve: {
    dedupe: ["@tanstack/db"],
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
