/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- This test runs and restarts a real workerd process. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Config, Effect } from "effect";
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault(tmpdir())),
);

const request = (
  instance: Miniflare,
  path: string,
  init?: Parameters<Miniflare["dispatchFetch"]>[1],
) => instance.dispatchFetch(`http://generation.test${path}`, init);

test("generation outputs route to the checkpoint object after restart and creation rolls back", async () => {
  const root = mkdtempSync(join(scratch, "generation-workerd-"));
  let mf: Miniflare | undefined;

  try {
    const built = await Bun.build({
      entrypoints: [join(import.meta.dirname, "support/generation-outputs-worker.ts")],
      outdir: join(root, "bundle"),
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
    });

    const output = built.outputs[0];

    if (!built.success || output === undefined) throw new Error("No generation workerd bundle");

    const start = (generation: number) =>
      new Miniflare({
        rootPath: root,
        modulesRoot: join(root, "bundle"),
        scriptPath: output.path,
        modules: true,
        compatibilityDate: "2026-08-06",
        cf: false,
        host: "127.0.0.1",
        port: 0,
        bindings: { GENERATION: generation },
        durableObjects: { OBJECT: { className: "GenerationObject", useSQLite: true } },
        durableObjectsPersist: join(root, "state"),
      });

    mf = start(1);

    const input = "/streams/g1/facts/one";
    const headers = { "content-type": "application/json" };
    expect((await request(mf, input, { method: "PUT", headers })).status).toBe(201);

    const append = await request(mf, input, {
      method: "POST",
      headers,
      body: JSON.stringify([
        { id: "a", n: 2 },
        { id: "a", n: 3 },
      ]),
    });

    expect(append.status).toBe(204);
    const offset = append.headers.get("stream-next-offset");

    const proof = "/proof/run";
    expect(await (await request(mf, proof)).json()).toEqual({
      checkpoint: true,
      count: 5,
      replayed: 2,
    });
    const numbers = "/streams/g1/numbers/one/g";
    const rows = "/streams/g1/rows/one/g";
    const accepted = "/streams/g1/accepted/one/g";
    const oldNumbers = await (await request(mf, `${numbers}/1?offset=-1`)).text();
    const oldRows = await (await request(mf, `${rows}/1?offset=-1`)).text();
    expect(JSON.parse(oldNumbers)).toEqual([2, 3]);
    expect(JSON.parse(oldRows)).toMatchObject([{ value: { id: "a", count: 5 } }]);

    await mf.dispose();
    mf = start(2);
    expect(await (await request(mf, "/proof/fail")).json()).toEqual({
      handlerFailed: true,
      checkpoint: false,
    });
    expect((await request(mf, `${numbers}/2`, { method: "HEAD" })).status).toBe(404);
    expect((await request(mf, `${accepted}/2`, { method: "HEAD" })).status).toBe(404);

    expect(await (await request(mf, proof)).json()).toEqual({
      checkpoint: true,
      count: 5,
      replayed: 2,
    });
    expect(await (await request(mf, proof)).json()).toEqual({
      checkpoint: true,
      count: 5,
      replayed: 0,
    });

    for (const generation of [1, 2]) {
      expect(await (await request(mf, `${numbers}/${generation}?offset=-1`)).text()).toBe(
        oldNumbers,
      );
      expect(await (await request(mf, `${rows}/${generation}?offset=-1`)).text()).toBe(oldRows);
      expect(await (await request(mf, `${accepted}/${generation}?offset=-1`)).json()).toMatchObject(
        [{ value: { stream: "g1/facts/one", offset } }],
      );
    }
  } finally {
    await mf?.dispose();
    rmSync(root, { recursive: true, force: true });
  }
}, 60_000);
