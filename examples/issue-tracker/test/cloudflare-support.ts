/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- Bun test owns build/workerd lifecycle at this executable edge. */
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Miniflare } from "miniflare";
import { Schema } from "effect";
import type { JsonValue } from "@streamsy/core";

export interface WorkerdHarness {
  readonly mf: Miniflare;
  readonly fetch: (
    path: string,
    init?: Parameters<Miniflare["dispatchFetch"]>[1],
  ) => ReturnType<Miniflare["dispatchFetch"]>;
  readonly evictWorkspace: (workspaceId: string) => Promise<void>;
  readonly runWorkspaceMaintenance: (workspaceId: string) => Promise<void>;
  readonly close: () => Promise<void>;
}

export async function workerdHarness(entrypoint = "server/cloudflare.ts"): Promise<WorkerdHarness> {
  // Miniflare 4.20260730 resolves DO persistence inside workerd's sandbox and
  // rejects an absolute path that escapes its starting directory.
  const root = mkdtempSync(".issue-tracker-workerd-");
  const outdir = join(root, "bundle");
  const built = await Bun.build({
    entrypoints: [entrypoint],
    outdir,
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });
  if (!built.success) throw new Error(built.logs.map(String).join("\n"));
  const output = built.outputs[0];
  if (output === undefined) throw new Error("Cloudflare test build produced no output");
  const mf = new Miniflare({
    scriptPath: output.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    durableObjects: entrypoint.includes("parity")
      ? { PARITY: { className: "SqlParityObject", useSQLite: true } }
      : { WORKSPACES: { className: "WorkspacePartitionObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
    bindings: { DEPLOYMENT: "workerd-test" },
    unsafeInspectDurableObjects: true,
  });
  await mf.ready;
  return {
    mf,
    fetch: (path, init) => mf.dispatchFetch(`http://issue-tracker.test${path}`, init),
    evictWorkspace: (workspaceId) =>
      mf.unsafeEvictDurableObject("", "WorkspacePartitionObject", {
        name: `workspace:${workspaceId}`,
      }),
    runWorkspaceMaintenance: async (workspaceId) => {
      const namespace = await mf.getDurableObjectNamespace("WORKSPACES");
      const stub = namespace.get(namespace.idFromName(`workspace:${workspaceId}`));
      const response = await stub.fetch("http://workspace.internal/_streamsy/maintenance", {
        headers: { "x-streamsy-partition-key": `workspace:${workspaceId}` },
      });
      if (!response.ok) {
        throw new Error(
          `workspace maintenance failed: ${response.status} ${await response.text()}`,
        );
      }
    },
    close: async () => {
      await mf.dispose();
      rmSync(root, { recursive: true, force: true });
    },
  };
}

export const jsonRequest = (
  method: string,
  body?: JsonValue,
): NonNullable<Parameters<Miniflare["dispatchFetch"]>[1]> => ({
  method,
  headers: body === undefined ? undefined : { "content-type": "application/json" },
  body: body === undefined ? undefined : JSON.stringify(body),
});

export async function decodeResponse<S extends Schema.ConstraintDecoder<unknown>>(
  response: Awaited<ReturnType<Miniflare["dispatchFetch"]>>,
  schema: S,
): Promise<S["Type"]> {
  const value: unknown = await response.json();
  return Schema.decodeUnknownSync(schema)(value);
}
