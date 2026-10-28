import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "happy-dom",
    setupFiles: ["./test/vitest-setup.ts"],
    server: {
      deps: {
        inline: ["foldkit", "@foldkit/ui", "@foldkit/devtools"],
      },
    },
  },
});
