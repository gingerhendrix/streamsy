// oxlint-disable-next-line effecttsgo/node-builtin-import -- This executable checks its built assets and creates its caller-selected screenshot directory through the native filesystem.
import { existsSync, mkdirSync } from "node:fs";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This executable resolves its package assets and screenshot paths through Bun's Node-compatible path API.
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { createLocalHost } from "../server/local.ts";
import { request } from "./http.ts";

const packageDir = join(dirname(new URL(import.meta.url).pathname), "..");
if (!existsSync(join(packageDir, "dist/assets/index.html"))) {
  throw new Error("Run `bun run build` before the UI smoke");
}

const playwrightModule =
  // oxlint-disable-next-line effecttsgo/process-env -- The executable accepts the installed Playwright module path through its documented process environment override.
  process.env.PLAYWRIGHT_MODULE ??
  "/home/gareth/.local/share/mise/installs/node/latest/lib/node_modules/@playwright/cli/node_modules/playwright-core/index.mjs";
const chromiumExecutable =
  // oxlint-disable-next-line effecttsgo/process-env -- The executable accepts the installed Chromium path through its documented process environment override.
  process.env.PLAYWRIGHT_EXECUTABLE ??
  "/home/gareth/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome";
if (!existsSync(playwrightModule) || !existsSync(chromiumExecutable)) {
  throw new Error("The UI smoke requires the installed Playwright module and Chromium executable");
}

/** The two Playwright shapes this script uses. The module itself is loaded dynamically. */
interface PlaywrightLocator {
  readonly waitFor: (options?: { readonly timeout?: number }) => Promise<void>;
}
interface PlaywrightPage {
  readonly locator: (selector: string) => PlaywrightLocator;
}

/** Select one board card without matching the activity panel's repeated issue title. */
const cardTitled = (target: PlaywrightPage, title: string): PlaywrightLocator =>
  target.locator(`li.card p.title:text-is("${title}")`);

const playwright = await import(pathToFileURL(playwrightModule).href);
const scratch =
  // oxlint-disable-next-line effecttsgo/process-env -- The executable's screenshot destination is a caller-owned command environment contract.
  process.env.STREAMSY_A5_SCRATCH ??
  "/home/gareth/Documents/Personal/scratch/2026-08-24-streamsy-a5-state-sink";
mkdirSync(scratch, { recursive: true });

// `exchange: interval` is what feeds the inbox panel: the browser cannot drive a
// host-level pass, so the demonstration runs the host the way a person would.
const host = createLocalHost({ delivery: { mode: "interval" }, exchange: { mode: "interval" } });
const server = Bun.serve({ port: 0, fetch: host.fetch, idleTimeout: 30 });
const origin = `http://localhost:${server.port}`;
const workspace = "browser-smoke";
const browser = await playwright.chromium.launch({ executablePath: chromiumExecutable });
const problems: string[] = [];

// oxlint-disable-next-line effecttsgo/async-function -- This Playwright adapter constructs a native browser page and attaches its Promise/event-based failure observers.
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
  const seeded = await request(`${origin}/api/workspaces/${workspace}/seed`, { method: "POST" });
  assert(seeded.ok, `seed failed: ${seeded.status}`);

  const left = await page(1440);
  const right = await page(900);
  await Promise.all([
    left.goto(`${origin}/?workspace=${workspace}`),
    right.goto(`${origin}/?workspace=${workspace}`),
  ]);
  await Promise.all([
    cardTitled(left, "Declare the issue view").waitFor(),
    cardTitled(right, "Declare the issue view").waitFor(),
  ]);
  assert((await left.locator(".badge > span").first().textContent()) === "Live", "left is live");
  assert((await right.locator(".badge > span").first().textContent()) === "Live", "right is live");

  await left.locator('input[name="title"]').fill("Two browser clients converge");
  await left.locator('select[name="status"]').selectOption("todo");
  await left.locator('button[type="submit"]').click();
  await Promise.all([
    cardTitled(left, "Two browser clients converge").waitFor(),
    cardTitled(right, "Two browser clients converge").waitFor(),
  ]);

  const leftCard = left.locator('li.card:has-text("Two browser clients converge")');
  await leftCard.locator("select.issue-status").selectOption("done");
  await right
    .locator('section[data-status="done"] li.card:has-text("Two browser clients converge")')
    .waitFor();

  const counts = await Promise.all([left.locator(".card").count(), right.locator(".card").count()]);
  assert(
    counts[0] === counts[1] && counts[0] >= 5,
    `rendered counts diverged: ${counts.join(" vs ")}`,
  );

  /**
   * The second checked State sink, driven the same way: attach a label in one
   * window and assert the *other* window's label count moves without a reload.
   * The seeded workspace already has `infra` on two issues, so the assertion is
   * a change rather than an appearance.
   */
  const infraBefore = Number(
    await right.locator('[data-count-label="infra"]').first().textContent(),
  );
  await leftCard.locator("select.add-label").selectOption("infra");
  await right.locator(`[data-count-label="infra"]:text-is("${String(infraBefore + 1)}")`).waitFor();
  await leftCard.locator('button.chip[data-label="infra"]').click();
  await right.locator(`[data-count-label="infra"]:text-is("${String(infraBefore)}")`).waitFor();

  /**
   * The cross-workspace inbox: a polled read model, so the assertion waits for a
   * refresh rather than for a push. Asserting it here is what keeps the product
   * claim honest — the panel is visible and it converges, just not live.
   */
  await request(`${origin}/api/workspaces/${workspace}/issues/seed-plan/assignee`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ commandId: "ui-smoke-assign", assigneeId: "ada" }),
  });
  await left.locator(`[data-inbox="${workspace}.ui-smoke-assign"]`).waitFor({ timeout: 15_000 });
  await right.locator(`[data-inbox="${workspace}.ui-smoke-assign"]`).waitFor({ timeout: 15_000 });

  const summary = await left.locator('[data-summary="total"]').first().textContent();
  assert(Number(summary) === counts[0], `summary total ${String(summary)} != ${counts[0]}`);

  assert(problems.length === 0, `browser problems: ${problems.join(" | ")}`);

  await left.screenshot({ path: join(scratch, "browser-left.png"), fullPage: true });
  await right.screenshot({ path: join(scratch, "browser-right.png"), fullPage: true });
  // oxlint-disable-next-line effecttsgo/global-console -- The UI smoke command's stdout contract reports its two-client rendered-card result.
  console.log(`issue-tracker UI smoke passed: two rendered clients, ${counts[0]} cards each`);
} finally {
  await browser.close();
  await server.stop(true);
  await host.close();
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
