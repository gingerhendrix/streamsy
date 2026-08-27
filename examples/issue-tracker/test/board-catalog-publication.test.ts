/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is HTTP-level convergence of a checked sink. */
/**
 * The board State sink after a catalog-only change.
 *
 * The board graph joins the project and user catalogs, so a pass in which no
 * issue event was folded can still produce new board rows. This suite drives
 * exactly that pass over HTTP and reads the checked sink back, because the sink
 * — not the graph — is what a person sees. It is the case that the issue-source
 * checkpoint cannot express: the checkpoint does not move, so a publisher keyed
 * on it publishes nothing, and every card whose issue does not happen to change
 * afterwards keeps serving the old name for as long as that stays true.
 *
 * Each test also checks a card nobody touched, because the failure this guards
 * against is per card: the previous behaviour healed a card only when that
 * card's own issue changed.
 */
import { afterEach, expect, test } from "bun:test";
import { CommandResponse } from "../shared/api.ts";
import { call, host, json, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

/** One Durable State message on the board sink, in the shape this suite reads. */
interface SinkMessage {
  readonly type?: string;
  readonly key?: string;
  readonly value?: { readonly projectName?: string; readonly status?: string };
}

async function messages(instance: Host): Promise<readonly SinkMessage[]> {
  const response = await call(instance, "GET", "/state/workspaces/main/issues");
  if (!response.ok) throw new Error(`board sink: ${response.status}`);
  // SAFETY: a 2xx from the declared sink route is a Durable State message
  // array; `SinkMessage` names only the optional fields read here.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  return JSON.parse(await response.text()) as SinkMessage[];
}

/**
 * The card set a resuming client would end up holding: last write per key.
 *
 * A snapshot re-upserts every row, so folding the whole message log this way
 * gives the same answer as replaying it through the State protocol, for the one
 * field these tests compare.
 */
async function cards(instance: Host): Promise<Record<string, string>> {
  const folded: Record<string, string> = {};
  for (const message of await messages(instance)) {
    if (message.type !== "issue" || message.key === undefined) continue;
    const name = message.value?.projectName;
    if (name !== undefined) folded[message.key] = name;
  }
  return folded;
}

/** Run a maintenance pass without changing any fact. */
const quietPass = (instance: Host) => call(instance, "GET", "/api/workspaces/main/issues");

const renameProject = (instance: Host, name: string) =>
  call(instance, "POST", "/api/workspaces/main/catalog/projects", {
    key: "streamsy",
    value: {
      projectId: "streamsy",
      workspaceId: "main",
      key: "STR",
      name,
      updatedAt: "2026-08-27T00:00:00.000Z",
    },
  });

const upsertUser = (instance: Host, name: string) =>
  call(instance, "POST", "/api/workspaces/main/catalog/users", {
    key: "ada",
    value: { userId: "ada", workspaceId: "main", name, updatedAt: "2026-08-27T00:00:00.000Z" },
  });

async function seeded(): Promise<Host> {
  const instance = host();
  open.push(instance);
  await call(instance, "POST", "/api/workspaces/main/seed");
  return instance;
}

test("a project rename with no issue change reaches every card on the board sink", async () => {
  const instance = await seeded();
  expect(await cards(instance)).toMatchObject({ "seed-plan": "Streamsy" });

  expect((await renameProject(instance, "Streamsy Platform")).status).toBe(200);
  await quietPass(instance);

  // Every card, not just the one belonging to some issue that changed. The
  // seed writes four, and none of their issues moved.
  const renamed = await cards(instance);
  expect(Object.keys(renamed).toSorted()).toEqual([
    "seed-maintain",
    "seed-plan",
    "seed-publish",
    "seed-scale",
  ]);
  for (const [key, name] of Object.entries(renamed)) {
    expect([key, name]).toEqual([key, "Streamsy Platform"]);
  }
});

test("a card whose own issue never changes does not stay stale behind one that does", async () => {
  const instance = await seeded();
  await renameProject(instance, "Streamsy Platform");

  // The card that *does* change is the one the old behaviour healed; the two
  // that do not are the ones it left serving the old name indefinitely.
  const moved = await call(instance, "POST", "/api/workspaces/main/issues/seed-publish/status", {
    commandId: "catalog-move-1",
    status: "in_progress",
  });
  expect(moved.status).toBe(200);
  await quietPass(instance);

  const after = await cards(instance);
  expect(after["seed-publish"]).toBe("Streamsy Platform");
  expect(after["seed-plan"]).toBe("Streamsy Platform");
  expect(after["seed-maintain"]).toBe("Streamsy Platform");
  expect(after["seed-scale"]).toBe("Streamsy Platform");
});

test("a user catalog change leaves the board sink authoritative and appends nothing spurious", async () => {
  const instance = await seeded();
  await quietPass(instance);
  const before = await messages(instance);

  // The board joins users but selects no user field, so a user-only change
  // moves the graph's input position without changing a single row. The sink
  // must neither go stale nor accumulate a republish per pass.
  expect((await upsertUser(instance, "Ada Lovelace")).status).toBe(200);
  await quietPass(instance);
  expect((await upsertUser(instance, "Ada L.")).status).toBe(200);
  await quietPass(instance);

  expect(await cards(instance)).toEqual(await cards(instance));
  expect((await messages(instance)).length).toBe(before.length);

  // And the board still publishes catalog changes that *do* move rows.
  await renameProject(instance, "Streamsy Platform");
  await quietPass(instance);
  const renamed = await cards(instance);
  expect(Object.values(renamed).toSorted()).toEqual([
    "Streamsy Platform",
    "Streamsy Platform",
    "Streamsy Platform",
    "Streamsy Platform",
  ]);
});

test("`publication: none` means the sink is already authoritative, not that work was skipped", async () => {
  const instance = await seeded();
  await quietPass(instance);
  const settled = await messages(instance);

  // A quiet pass appends nothing, and reports that as `none`.
  const quiet = await json(
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/status", {
      commandId: "publication-1",
      status: "todo",
    }),
    CommandResponse,
  );
  expect(quiet.maintenance.publication).toBe("changes");

  const repeated = await json(
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/status", {
      commandId: "publication-1",
      status: "todo",
    }),
    CommandResponse,
  );
  // A reconciled command folds nothing and the graph revision does not move,
  // so the sink is already carrying it.
  expect(repeated.reconciled).toBe(true);
  expect(repeated.maintenance.publication).toBe("none");
  expect((await messages(instance)).length).toBeGreaterThan(settled.length);

  const stable = (await messages(instance)).length;
  await quietPass(instance);
  await quietPass(instance);
  expect((await messages(instance)).length).toBe(stable);
});
