/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/new-promise, effecttsgo/global-timers, effecttsgo/global-date, effecttsgo/node-builtin-import, effecttsgo/process-env -- Black-box provider/workerd tests own native HTTP, timing and scratch storage. */
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { YjsProvider } from "@durable-streams/y-durable-streams";
import * as encoding from "lib0/encoding";
import { Awareness } from "y-protocols/awareness";
import * as Y from "yjs";
import { startWorker } from "../scripts/worker-harness.ts";

const scratch = process.env.YJS_TEST_SCRATCH ?? `${import.meta.dir}/../.scratch/tests`;

const wait = async (condition: () => boolean | Promise<boolean>) => {
  const deadline = Date.now() + 10000;

  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }

  throw new Error("Timed out waiting for provider/Worker");
};

const frame = (update: Uint8Array) => {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint8Array(encoder, update);

  return encoding.toUint8Array(encoder);
};

describe("native Cloudflare Yjs host", () => {
  let directory: string;
  let host: Awaited<ReturnType<typeof startWorker>>;
  const peers: Array<{ provider: YjsProvider; doc: Y.Doc; awareness?: Awareness }> = [];

  const peer = (docId: string, awarenessEnabled = false) => {
    const doc = new Y.Doc();
    const awareness = awarenessEnabled ? new Awareness(doc) : undefined;
    const provider = new YjsProvider({ doc, awareness, baseUrl: `${host.url}/v1/yjs/test`, docId });
    const result = { doc, awareness, provider };
    peers.push(result);

    return result;
  };

  beforeAll(async () => {
    await mkdir(scratch, { recursive: true });
    directory = await mkdtemp(`${scratch}/worker-`);
    host = await startWorker(directory, 0, 1500);
  });
  afterAll(async () => {
    for (const item of peers) {
      item.provider.destroy();
      item.awareness?.destroy();
      item.doc.destroy();
    }

    await host?.stop();
    await rm(directory, { recursive: true, force: true });
  });

  it("two stock providers converge after concurrent edits over binary SSE", async () => {
    const a = peer("converge");
    const b = peer("converge");
    await wait(() => a.provider.synced && b.provider.synced);
    a.doc.getText("text").insert(0, "alpha ");
    b.doc.getText("text").insert(0, "beta ");
    await a.provider.flush();
    await b.provider.flush();
    await wait(
      () =>
        a.doc.getText("text").toJSON() === b.doc.getText("text").toJSON() &&
        a.doc.getText("text").length === 11,
    );
    expect(a.doc.getText("text").toJSON()).toContain("alpha");
    await b.provider.disconnect();
    a.doc.getText("text").insert(11, "still editing");
    await a.provider.flush();
    const fresh = peer("converge");
    await wait(() => fresh.provider.synced);
    expect(fresh.doc.getText("text").toJSON()).toBe(a.doc.getText("text").toJSON());
    fresh.provider.destroy();
    a.provider.destroy();
    b.provider.destroy();
  });

  it("compacts and a fresh stock provider loads snapshot plus the following update", async () => {
    const a = peer("snapshot");
    await wait(() => a.provider.synced);
    a.doc.getText("text").insert(0, "S".repeat(1800));
    await a.provider.flush();
    const docUrl = `${host.url}/v1/yjs/test/docs/snapshot`;
    const redirect = await fetch(`${docUrl}?offset=snapshot`, { redirect: "manual" });
    expect(redirect.status).toBe(307);
    expect(redirect.headers.get("location")).toContain("_snapshot");
    a.doc.getText("text").insert(1800, " tail");
    await a.provider.flush();
    const b = peer("snapshot");
    await wait(() => b.provider.synced);
    expect(b.doc.getText("text").toJSON()).toBe(`${"S".repeat(1800)} tail`);
    a.provider.destroy();
    b.provider.destroy();
  });

  it("persists both a partial compaction counter and the published snapshot across workerd restarts", async () => {
    const docId = "restart";
    const doc = new Y.Doc();
    const docUrl = () => `${host.url}/v1/yjs/test/docs/${docId}`;
    await fetch(docUrl(), { method: "PUT" });
    let pending = new Uint8Array();
    doc.on("update", (update: Uint8Array) => {
      pending = frame(update);
    });
    doc.getText("text").insert(0, "A".repeat(900));
    expect((await fetch(docUrl(), { method: "POST", body: pending })).ok).toBe(true);
    expect(
      (await fetch(`${docUrl()}?offset=snapshot`, { redirect: "manual" })).headers.get("location"),
    ).toContain("offset=-1");
    // dispose/recreate workerd, keeping only the SQLite directory.
    await host.stop();
    host = await startWorker(directory, 0, 1500);
    doc.getText("text").insert(900, "B".repeat(900));
    // Seed the append/fold gap through the backing route, then stop workerd.
    // Discovery after restart must fold this unprocessed tail with the saved state.
    expect(
      (
        await fetch(`${host.url}/v1/stream/yjs/test/docs/${docId}/.updates`, {
          method: "POST",
          body: pending,
          headers: { "content-type": "application/octet-stream" },
        })
      ).ok,
    ).toBe(true);
    await host.stop();
    host = await startWorker(directory, 0, 1500);

    const snapshot = (
      await fetch(`${docUrl()}?offset=snapshot`, { redirect: "manual" })
    ).headers.get("location");

    expect(snapshot).toContain("_snapshot");
    await host.stop();
    host = await startWorker(directory, 0, 1500);
    expect(
      (await fetch(`${docUrl()}?offset=snapshot`, { redirect: "manual" })).headers.get("location"),
    ).toBe(snapshot);
    const b = peer(docId);
    await wait(() => b.provider.synced);
    expect(b.doc.getText("text").toJSON()).toBe(`${"A".repeat(900)}${"B".repeat(900)}`);
    b.provider.destroy();
    doc.destroy();
  });

  it("serializes concurrent compactions without losing either writer's updates", async () => {
    const a = peer("concurrent-compaction");
    const b = peer("concurrent-compaction");
    await wait(() => a.provider.synced && b.provider.synced);
    a.doc.getText("text").insert(0, "A".repeat(1800));
    b.doc.getText("text").insert(0, "B".repeat(1800));
    await Promise.all([a.provider.flush(), b.provider.flush()]);
    await wait(
      () => a.doc.getText("text").length === 3600 && b.doc.getText("text").length === 3600,
    );
    const fresh = peer("concurrent-compaction");
    await wait(() => fresh.provider.synced);
    expect(fresh.doc.getText("text").toJSON()).toBe(a.doc.getText("text").toJSON());
    a.provider.destroy();
    b.provider.destroy();
    fresh.provider.destroy();
  });

  it("delivers awareness to a second stock provider and gives named streams a TTL", async () => {
    const a = peer("presence", true);
    const b = peer("presence", true);
    await wait(() => a.provider.synced && b.provider.synced);
    // Allow offset=now subscriptions to attach before broadcasting.
    await wait(
      async () =>
        (await fetch(`${host.url}/v1/yjs/test/docs/presence?awareness=default`, { method: "HEAD" }))
          .ok,
    );
    a.awareness!.setLocalStateField("user", { name: "Ada", color: "#276b64" });
    await wait(() => b.awareness!.getStates().get(a.doc.clientID)?.user?.name === "Ada");
    const url = `${host.url}/v1/yjs/test/docs/presence?awareness=custom`;
    expect((await fetch(url, { method: "PUT" })).status).toBe(201);
    expect((await fetch(url, { method: "HEAD" })).headers.get("stream-ttl")).toBe("3600");
    a.provider.destroy();
    b.provider.destroy();
  });
});
