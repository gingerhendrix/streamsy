import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  root: ".",
  server: {
    port: 5174,
    proxy: {
      "/api": "http://localhost:1338",
      "/streams": "http://localhost:1338",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
