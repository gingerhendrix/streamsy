/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/global-fetch, effecttsgo/node-builtin-import, effecttsgo/process-env -- This executable starts a disposable host and drives the installed Playwright Chromium runtime. */
import { existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { createLocalHost } from "../server/local.ts";

const packageDir = join(dirname(new URL(import.meta.url).pathname), "..");
if (!existsSync(join(packageDir, "dist/assets/index.html"))) {
  throw new Error("Run `bun run build` before the UI smoke");
}

const playwrightModule =
  process.env.PLAYWRIGHT_MODULE ??
  "/home/gareth/.local/share/mise/installs/node/latest/lib/node_modules/@playwright/cli/node_modules/playwright-core/index.mjs";
const chromiumExecutable =
  process.env.PLAYWRIGHT_EXECUTABLE ??
  "/home/gareth/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome";
if (!existsSync(playwrightModule) || !existsSync(chromiumExecutable)) {
  throw new Error("The UI smoke requires the installed Playwright module and Chromium executable");
}

const playwright = await import(pathToFileURL(playwrightModule).href);
const scratch =
  process.env.STREAMSY_A5_SCRATCH ??
  "/home/gareth/Documents/Personal/scratch/2026-08-24-streamsy-a5-state-sink";
mkdirSync(scratch, { recursive: true });

const host = createLocalHost();
const server = Bun.serve({ port: 0, fetch: host.fetch, idleTimeout: 30 });
const origin = `http://localhost:${server.port}`;
const workspace = "browser-smoke";
const browser = await playwright.chromium.launch({ executablePath: chromiumExecutable });
const problems: string[] = [];

async function page(width: number) {
  const context = await browser.newContext({ viewport: { width, height: 900 } });
  const opened = await context.newPage();
  opened.on("console", (message) => {
    if (message.type() === "error") problems.push(`console: ${message.text()}`);
  });
  opened.on("pageerror", (error) => problems.push(`pageerror: ${String(error)}`));
  opened.on("response", (response) => {
    if (response.status() >= 400) problems.push(`${response.status()} ${response.url()}`);
  });
  return opened;
}

try {
  const seeded = await fetch(`${origin}/api/workspaces/${workspace}/seed`, { method: "POST" });
  assert(seeded.ok, `seed failed: ${seeded.status}`);

  const left = await page(1440);
  const right = await page(900);
  await Promise.all([
    left.goto(`${origin}/?workspace=${workspace}`),
    right.goto(`${origin}/?workspace=${workspace}`),
  ]);
  await Promise.all([
    left.getByText("Declare the issue view", { exact: true }).waitFor(),
    right.getByText("Declare the issue view", { exact: true }).waitFor(),
  ]);
  assert((await left.locator(".badge > span").first().textContent()) === "Live", "left is live");
  assert((await right.locator(".badge > span").first().textContent()) === "Live", "right is live");

  await left.locator('input[name="title"]').fill("Two browser clients converge");
  await left.locator('select[name="status"]').selectOption("todo");
  await left.locator('button[type="submit"]').click();
  await Promise.all([
    left.getByText("Two browser clients converge", { exact: true }).waitFor(),
    right.getByText("Two browser clients converge", { exact: true }).waitFor(),
  ]);

  const leftCard = left.locator('li.card:has-text("Two browser clients converge")');
  await leftCard.locator("select").selectOption("done");
  await right
    .locator('section[data-status="done"] li.card:has-text("Two browser clients converge")')
    .waitFor();

  const counts = await Promise.all([left.locator(".card").count(), right.locator(".card").count()]);
  assert(
    counts[0] === counts[1] && counts[0] >= 5,
    `rendered counts diverged: ${counts.join(" vs ")}`,
  );
  assert(problems.length === 0, `browser problems: ${problems.join(" | ")}`);

  await left.screenshot({ path: join(scratch, "browser-left.png"), fullPage: true });
  await right.screenshot({ path: join(scratch, "browser-right.png"), fullPage: true });
  console.log(`issue-tracker UI smoke passed: two rendered clients, ${counts[0]} cards each`);
} finally {
  await browser.close();
  await server.stop(true);
  await host.close();
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
