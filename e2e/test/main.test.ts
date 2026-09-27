import { expect, test } from "bun:test";

test("help is side-effect free and exits successfully", async () => {
  const child = Bun.spawn(["bun", "run", "src/main.ts", "--help"], {
    cwd: new URL("..", import.meta.url).pathname,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, exitCode] = await Promise.all([child.stdout.text(), child.exited]);
  expect(exitCode).toBe(0);
  expect(stdout).toContain("streamsy deployed e2e");
  expect(stdout).toContain("runs the official Durable Streams suite");
});
