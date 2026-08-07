/**
 * Browser smoke over a production build of the UI.
 *
 * It boots the local host on an ephemeral port, seeds a disposable workspace,
 * and drives the real flows: keyboard issue creation, drawer edits, comments,
 * the accessible status control, the projection inspector, a reload that must
 * rebuild from durable State, and a second window that must converge without a
 * reload. It fails on any console error or failed application request.
 *
 * Playwright is deliberately not a repository dependency, so this file is also
 * excluded from `typecheck`. Install the driver where you want to run it:
 *
 *   bun add -g playwright-core && bunx playwright install chromium
 *   PLAYWRIGHT_EXECUTABLE=<chrome binary> bun run smoke:ui
 *
 * Without playwright-core installed the script skips and exits 0.
 */
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createLocalHost } from "../server/local.ts";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
if (!existsSync(join(packageDir, "dist/assets/index.html"))) {
  throw new Error("Run `bun run build` before the UI smoke");
}

let chromium: typeof import("playwright-core").chromium;
try {
  ({ chromium } = await import("playwright-core"));
} catch {
  console.log("smoke:ui skipped — playwright-core is not installed in this environment");
  process.exit(0);
}

const host = createLocalHost();
const server = Bun.serve({ port: 0, fetch: host.fetch, idleTimeout: 30 });
const base = `http://localhost:${server.port}`;
const workspaceId = "uismoke";

const browser = await chromium.launch(
  process.env.PLAYWRIGHT_EXECUTABLE === undefined
    ? {}
    : { executablePath: process.env.PLAYWRIGHT_EXECUTABLE },
);

const problems: string[] = [];

async function open(width: number, height: number) {
  const context = await browser.newContext({ viewport: { width, height } });
  const page = await context.newPage();
  page.on("console", (message) => {
    if (message.type() === "error") problems.push(`console: ${message.text()}`);
  });
  page.on("pageerror", (error) => problems.push(`pageerror: ${String(error)}`));
  page.on("response", (response) => {
    if (response.status() >= 400) problems.push(`${response.status()} ${response.url()}`);
  });
  return page;
}

try {
  const seeded = await fetch(`${base}/api/workspaces/${workspaceId}/seed`, { method: "POST" });
  assert(seeded.ok, "the workspace must seed");

  const page = await open(1440, 900);
  await page.goto(`${base}/?workspace=${workspaceId}&project=launch`);
  await page.waitForSelector("[data-testid=board]");
  await page.waitForFunction(() => document.querySelectorAll(".card").length >= 3);
  assert(
    (await page.textContent("[data-testid=connection-state]")) === "Live",
    "the board feed must report Live",
  );

  // Creation is completable from the keyboard alone.
  await page.click("[data-testid=add-issue-backlog]");
  await page.fill("[data-testid=new-issue-input-backlog]", "Smoke created issue");
  await page.keyboard.press("Enter");
  await page.waitForSelector('.card:has-text("Smoke created issue")');
  await page.waitForFunction(
    () =>
      [...document.querySelectorAll<HTMLElement>(".card")].some(
        (card) =>
          card.querySelector(".card-title")?.textContent === "Smoke created issue" &&
          card.dataset.sync === "synced",
      ),
    undefined,
    { timeout: 20_000 },
  );

  // Drawer edits and a comment, settling to a durable Synced state.
  await page.locator('.card:has-text("Ship the projection inspector") .card-open').click();
  await page.waitForSelector("[data-testid=issue-title]");
  await page.selectOption("[data-testid=issue-priority]", "urgent");
  await page.fill("[data-testid=new-comment]", "Proven through both hops.");
  await page.click("[data-testid=submit-comment]");
  await page.waitForSelector(".drawer .chip.ok", { timeout: 20_000 });
  await page.waitForFunction(() => document.querySelectorAll(".comment-list li").length >= 2);

  // The accessible status control is a first-class equal of dragging.
  await page.selectOption("[data-testid=issue-status]", "done");
  await page.waitForFunction(
    () =>
      (document.querySelector("[data-testid=issue-status]") as HTMLSelectElement | null)?.value ===
      "done",
  );
  await page.click("[data-testid=close-drawer]");
  await page.waitForTimeout(300);
  assert(
    (await page.evaluate(() => document.activeElement?.className ?? "")).includes("card-open"),
    "closing the drawer must return focus to its card",
  );

  // The inspector reports proven chained coverage for the latest command.
  await page.click("[data-testid=open-inspector]");
  await page.waitForSelector("[data-testid=inspector]");
  const badge = await page.locator(".mutation-list .badge").first().textContent();
  assert(badge === "Proven", `the latest command must be proven, got ${String(badge)}`);
  const hops = await page.locator(".hop h3").allTextContents();
  assert(
    hops.length === 3,
    `the inspector must label three durable identities, got ${hops.length}`,
  );
  await page.click("[data-testid=close-inspector]");

  // A reload must rebuild from the durable board State stream.
  await page.reload();
  await page.waitForSelector("[data-testid=board]");
  await page.waitForFunction(() =>
    [...document.querySelectorAll(".card-title")].some(
      (node) => node.textContent === "Smoke created issue",
    ),
  );
  const done = await page.locator("[data-testid=column-done] .card-title").allTextContents();
  assert(
    done.includes("Ship the projection inspector"),
    "the status change must survive a reload without a command cache",
  );

  // A second window converges through the shared stream, with no reload.
  const second = await open(760, 800);
  await second.goto(`${base}/?workspace=${workspaceId}&project=launch`);
  await second.waitForSelector("[data-testid=board]");
  await page.click("[data-testid=add-issue-backlog]");
  await page.fill("[data-testid=new-issue-input-backlog]", "Second window issue");
  await page.keyboard.press("Enter");
  await second.waitForSelector('.card:has-text("Second window issue")', { timeout: 25_000 });

  // Mobile keeps the sheet and the status control usable.
  const mobile = await open(390, 844);
  await mobile.goto(`${base}/?workspace=${workspaceId}&project=launch`);
  await mobile.waitForSelector("[data-testid=board]");
  await mobile.locator(".card .card-open").first().click();
  await mobile.waitForSelector("[data-testid=issue-title]", { timeout: 15_000 });
  await mobile.waitForSelector("[data-testid=issue-status]");

  assert(problems.length === 0, `browser problems: ${problems.join(" | ")}`);
  console.log("issue-tracker-projections ui smoke passed");
} finally {
  await browser.close();
  server.stop(true);
  await host.close();
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
