import { defineConfig } from "vite";

export default defineConfig({
  base: "./",
  publicDir: false,
  build: { outDir: "public/web", emptyOutDir: true },
  server: {
    port: 5179,
    proxy: { "/api": "http://localhost:1339", "/streams": "http://localhost:1339" },
  },
});
