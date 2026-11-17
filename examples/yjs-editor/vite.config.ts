import { defineConfig } from "vite";

export default defineConfig({
  base: "./",
  publicDir: false,
  build: { outDir: "public/web", emptyOutDir: true },
  server: { port: 5180, proxy: { "/v1": "http://127.0.0.1:1340" } },
});
