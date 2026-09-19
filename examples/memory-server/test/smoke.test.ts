/* oxlint-disable effecttsgo/async-function -- The test runs the executable HTTP smoke at the process boundary. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The test resolves its package directory with the Node-compatible path API.
import { resolve } from "node:path";
import { expect, test } from "bun:test";

test("serves the HTTP walkthrough and starts empty after restart", async () => {
  const child = Bun.spawn(["bun", "run", "scripts/http-smoke.ts"], {
    cwd: resolve(import.meta.dir, ".."),
    stdout: "pipe",
    stderr: "pipe",
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);

  expect(stderr).toBe("");
  expect(exitCode).toBe(0);
  expect(stdout).toContain("including fresh-store restart");
}, 20_000);
