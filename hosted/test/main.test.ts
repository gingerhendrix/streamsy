import { expect, test } from "bun:test";

const runMain = async (...args: Array<string>) => {
  const process = Bun.spawn(["bun", "run", "src/main.ts", ...args], {
    cwd: new URL("..", import.meta.url).pathname,
    env: {
      ...Bun.env,
      STREAMSY_HOSTED_PERMISSION: "approved",
      STREAMSY_HOSTED_CREDENTIAL: "fake",
      SERVER_BASE_URL: "https://example.workers.dev/",
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    process.stdout.text(),
    process.stderr.text(),
    process.exited,
  ]);
  return { stdout, stderr, exitCode };
};

test("help is side-effect free and exits successfully", async () => {
  const result = await runMain("--help");
  expect(result.exitCode).toBe(0);
  expect(result.stdout).toContain("streamsy hosted evidence preparation");
  expect(result.stdout).toContain("Hosted execution and acceptance remain blocked");
});

test("evidence remains blocked even with populated fake opt-in inputs", async () => {
  const result = await runMain("evidence");
  expect(result.exitCode).toBe(2);
  expect(result.stderr).toContain("Live hosted adapters are not enabled in this local range");
  expect(result.stderr).toContain("27,160 B proposal");
});
