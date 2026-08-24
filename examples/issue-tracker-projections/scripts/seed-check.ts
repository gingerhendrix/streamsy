/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console -- This Bun check executable is a Promise-native driver over the local host's public HTTP surface and reports its single result line to the invoking terminal. */
/**
 * Seed the deterministic `main` workspace twice and verify that seeding is
 * idempotent: the same issues, the same board, no duplicated rows.
 */
import { createLocalHost } from "../server/local.ts";
import { SEEDED_ISSUE_IDS, SEEDED_PROJECT_IDS } from "../server/seed.ts";
import { BoardResponse, SeedResponse } from "../shared/api.ts";
import { Schema } from "effect";

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
    const board = await get(`/api/workspaces/main/projects/${projectId}/board`, BoardResponse);
    const keys = new Set(board.rows.map((row) => row.issueId));
    assert(keys.size === board.rows.length, `${projectId} board must not repeat rows`);
    total += board.rows.length;
  }
  assert(total === SEEDED_ISSUE_IDS.length, "every seeded issue must reach exactly one board");

  console.log(`seed check passed: ${SEEDED_PROJECT_IDS.length} projects, ${total} issues`);
} finally {
  await host.close();
}

async function post(path: string): Promise<SeedResponse> {
  return unwrap(
    await host.fetch(new Request(`http://localhost${path}`, { method: "POST" })),
    SeedResponse,
  );
}

async function get<S extends Schema.ConstraintDecoder<unknown>>(
  path: string,
  schema: S,
): Promise<S["Type"]> {
  return unwrap(await host.fetch(new Request(`http://localhost${path}`)), schema);
}

async function unwrap<S extends Schema.ConstraintDecoder<unknown>>(
  response: Response,
  schema: S,
): Promise<S["Type"]> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 400)}`);
  return Schema.decodeUnknownSync(schema)(JSON.parse(text));
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
