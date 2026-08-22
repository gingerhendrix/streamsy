/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console -- This Bun check executable is a Promise-native driver over the local host's public HTTP surface and reports its single result line to the invoking terminal. */
/**
 * Seed the deterministic `main` workspace twice and verify that seeding is
 * idempotent: the same issues, the same board, no duplicated rows.
 */
import { createLocalHost } from "../server/local.ts";
import { SEEDED_ISSUE_IDS, SEEDED_PROJECT_IDS } from "../server/seed.ts";
import type { BoardResponse } from "../shared/api.ts";

const host = createLocalHost();

try {
  const first = await post(`/api/workspaces/main/seed`);
  const second = await post(`/api/workspaces/main/seed`);
  assert(
    JSON.stringify(first) === JSON.stringify(second),
    "seeding twice must produce the same report",
  );
  assert(first.projects.length === SEEDED_PROJECT_IDS.length, "seed must create every project");
  assert(first.issues.length === SEEDED_ISSUE_IDS.length, "seed must create every issue");

  let total = 0;
  for (const projectId of SEEDED_PROJECT_IDS) {
    const board: BoardResponse = await get(`/api/workspaces/main/projects/${projectId}/board`);
    const keys = new Set(board.rows.map((row) => row.issueId));
    assert(keys.size === board.rows.length, `${projectId} board must not repeat rows`);
    total += board.rows.length;
  }
  assert(total === SEEDED_ISSUE_IDS.length, "every seeded issue must reach exactly one board");

  console.log(`seed check passed: ${SEEDED_PROJECT_IDS.length} projects, ${total} issues`);
} finally {
  await host.close();
}

async function post(path: string): Promise<any> {
  return unwrap(await host.fetch(new Request(`http://localhost${path}`, { method: "POST" })));
}

async function get(path: string): Promise<any> {
  return unwrap(await host.fetch(new Request(`http://localhost${path}`)));
}

async function unwrap(response: Response): Promise<any> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 400)}`);
  return JSON.parse(text);
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
