import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    exclude: [...configDefaults.exclude, "test/worker/**", "test/agent/worker.test.ts"],
    globalSetup: ["./test/setup.ts"],
  },
});
