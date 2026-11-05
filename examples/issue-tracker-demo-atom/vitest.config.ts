import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "happy-dom",
    environmentOptions: { happyDOM: { url: "http://localhost:1338/" } },
  },
});
