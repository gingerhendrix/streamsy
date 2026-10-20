import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // --dir test changes the glob base, so these work from either the root or test directory.
    // D11 defers the Worker host and its tests to batch 2.
    exclude: [...configDefaults.exclude, "**/worker/**", "**/agent/worker.test.ts"],
    globalSetup: ["./test/setup.ts"],
  },
});
