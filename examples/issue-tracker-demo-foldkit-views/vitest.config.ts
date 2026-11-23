import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "happy-dom",
    // The test contacts the loopback child directly without the Vite proxy.
    environmentOptions: {
      happyDOM: { settings: { fetch: { disableSameOriginPolicy: true } } },
    },
    setupFiles: ["./test/vitest-setup.ts"],
    server: {
      deps: {
        inline: ["foldkit", "@foldkit/ui", "@foldkit/devtools"],
      },
    },
  },
});
