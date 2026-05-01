import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  root: ".",
  server: {
    port: 5175,
    proxy: {
      "/api": "http://localhost:1339",
      "/streams": "http://localhost:1339",
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
