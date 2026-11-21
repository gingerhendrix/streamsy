import { defineConfig } from "vitest/config";

/** Real local workerd tests. Run with `bun run test:workerd`, which builds first. */
export default defineConfig({
  test: {
    include: ["test/workerd/**/*.test.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    fileParallelism: false,
  },
});
