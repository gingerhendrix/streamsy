/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/global-fetch, effecttsgo/strict-effect-provide -- These tests own the real local workerd boundary and run each client program at its own entry point. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test } from "vitest";
import { Array, Duration, Effect, Layer, Option, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { DurableState, StreamId, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { Miniflare } from "miniflare";
import { Files, UserWake } from "./support/wake-streams.ts";

/**
 * One folder object and one user object in a local workerd. A folder write
 * reaches the user's wake stream through Projection.wake, run by a
 * ProjectionWakeup on the folder object, over the user object stub.
 */

const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const cleanup of Array.reverse(cleanups.splice(0))) await cleanup();
});

const bundle = async (root: string) => {
  const built = await Bun.build({
    entrypoints: [join(import.meta.dirname, "support/wake-worker.ts")],
    outdir: join(root, "bundle"),
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  const output = built.outputs[0];

  if (!built.success || output === undefined) throw new Error("No workerd bundle");

  return output.path;
};

/** Start workerd over the persisted state in `root`. A second start over the same root is a restart. */
const start = async (root: string, scriptPath: string) => {
  const miniflare = new Miniflare({
    rootPath: root,
    modulesRoot: join(root, "bundle"),
    scriptPath,
    modules: true,
    compatibilityDate: "2026-08-06",
    host: "127.0.0.1",
    port: 0,
    cf: false,
    durableObjects: {
      FOLDERS: { className: "FolderObject", useSQLite: true },
      USERS: { className: "UserObject", useSQLite: true },
      CLASH: { className: "ClashObject", useSQLite: true },
    },
    durableObjectsPersist: join(root, "state"),
  });

  const url = await miniflare.ready;
  let disposed = false;

  const dispose = async () => {
    if (disposed) return;
    disposed = true;
    await miniflare.dispose();
  };

  cleanups.push(dispose);

  const send = async (path: string, init?: RequestInit) => {
    const response = await fetch(new URL(path, url), init);

    return { status: response.status, body: await response.text() };
  };

  return { url, send, dispose };
};

const workspace = async () => {
  const root = mkdtempSync(join(tmpdir(), "projection-wake-workerd-"));
  cleanups.push(() => rmSync(root, { recursive: true, force: true }));

  return { root, scriptPath: await bundle(root) };
};

const json = { "content-type": "application/json" };

/** Create the wake stream of `ann` on its owner, then the folder object and its `Files` stream. */
const setup = async (send: Awaited<ReturnType<typeof start>>["send"]) => {
  expect((await send("/__create-wake/ann")).status).toBe(201);
  expect(
    (await send("/streams/users/ann/folders/docs/files", { method: "PUT", headers: json })).status,
  ).toBe(201);
};

/** The wake rows of `ann`, read through the public mount. */
const rowsOf = async (url: URL) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const wake = yield* UserWake;
      const { items } = yield* wake.collect({ user: "ann" });
      const state = DurableState.materialize(DurableState.materialized(UserWake), items);

      return state.rows.stream;
    }).pipe(
      Effect.provide(
        RemoteStreams.layer(Streamsy.group(UserWake), { baseUrl: url.origin }).pipe(
          Layer.provide(FetchHttpClient.layer),
        ),
      ),
      Effect.orElseSucceed(() => new Map()),
    ),
  );

const until = async <A>(probe: () => Promise<A | undefined>, label: string): Promise<A> => {
  for (let attempt = 0; attempt < 200; attempt++) {
    const value = await probe();

    if (value !== undefined) return value;
    await Bun.sleep(50);
  }

  throw new Error(`Timed out waiting for ${label}`);
};

const sourceId = Files.at({ user: "ann", folderId: "docs" }).id;

test("a folder write reaches the user wake through the object stub", async () => {
  const { root, scriptPath } = await workspace();
  const { url, send } = await start(root, scriptPath);
  await setup(send);

  const appended = await send("/streams/users/ann/folders/docs/files", {
    method: "POST",
    headers: json,
    body: '"a.txt"',
  });

  expect(appended.status).toBe(204);

  const first = await until(async () => (await rowsOf(url)).get(sourceId), "the first wake row");

  expect(first.stream).toBe(sourceId);

  // A client follows the user wake and sees the folder row move, then go.
  const layer = RemoteStreams.layer(Streamsy.group(UserWake), { baseUrl: url.origin }).pipe(
    Layer.provide(FetchHttpClient.layer),
  );

  const followed = Effect.runPromise(
    Streamsy.followWake(
      UserWake,
      { user: "ann" },
      { cursors: new Map([[StreamId.make(sourceId), first.offset]]) },
    ).pipe(
      Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
      Stream.filter((item) => item._tag !== "Dirty" || item.offset !== first.offset),
      Stream.take(2),
      Stream.runCollect,
      Effect.timeoutOption(Duration.seconds(20)),
      Effect.provide(layer),
    ),
  );

  await Bun.sleep(200);
  expect(
    (
      await send("/streams/users/ann/folders/docs/files", {
        method: "POST",
        headers: json,
        body: '"b"',
      })
    ).status,
  ).toBe(204);
  await until(async () => {
    const row = (await rowsOf(url)).get(sourceId);

    return row !== undefined && row.offset !== first.offset ? row : undefined;
  }, "the second wake row");
  expect((await send("/streams/users/ann/folders/docs/files", { method: "DELETE" })).status).toBe(
    204,
  );

  const items = Option.getOrThrow(await followed);
  expect(Array.map(items, (item) => item._tag)).toEqual(["Dirty", "Removed"]);
  expect((await rowsOf(url)).size).toBe(0);

  // Clients cannot write the wake: the public mount reads only, and the Worker
  // does not route the internal mount.
  expect(
    (await send("/users/ann/wake", { method: "POST", headers: json, body: "[]" })).status,
  ).toBe(405);
  expect(
    (await send("/__wake/users/ann/wake", { method: "POST", headers: json, body: "[]" })).status,
  ).toBe(404);
}, 60_000);

test("after a restart the next source commit catches up an unarmed binding", async () => {
  const { root, scriptPath } = await workspace();
  const before = await start(root, scriptPath);
  await setup(before.send);

  // Settle the create notification before simulating the unarmed write.
  await until(
    async () => ((await before.send("/__folder/docs/alarm")).body === "none" ? true : undefined),
    "the create alarm to drain",
  );
  // A GET write arms no alarm. The restart stands in for a crash after the commit.
  const quiet = await before.send("/__quiet-append/ann/docs/b");
  expect(quiet.status).toBe(200);
  expect((await before.send("/__folder/docs/alarm")).body).toBe("none");
  expect((await rowsOf(before.url)).get(sourceId)).toBeUndefined();
  await before.dispose();

  const after = await start(root, scriptPath);
  // The pending set is in memory. Acquisition alone cannot rediscover the binding.
  expect((await after.send("/__folder/docs/alarm")).body).toBe("none");
  expect((await rowsOf(after.url)).get(sourceId)).toBeUndefined();
  // A close with no body is the next commit and must catch up the quiet append too.
  expect(
    (
      await after.send("/streams/users/ann/folders/docs/files", {
        method: "POST",
        headers: { ...json, "stream-closed": "true" },
      })
    ).status,
  ).toBe(204);

  const row = await until(async () => {
    const found = (await rowsOf(after.url)).get(sourceId);

    return found?.offset === quiet.body ? found : undefined;
  }, "the wake row after the restart");

  expect(row).toEqual({ stream: sourceId, offset: quiet.body, closed: true });
}, 60_000);

test("an object whose storage group also holds a linked stream refuses to start", async () => {
  const { root, scriptPath } = await workspace();
  const { send } = await start(root, scriptPath);

  const created = await send("/clash/folders/docs/files", { method: "PUT", headers: json });

  expect(created.status).toBe(503);
  expect(created.body).toBe("Storage unavailable");
}, 60_000);
