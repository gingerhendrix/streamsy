/**
 * Storage-adapter contract against the Durable Object adapter.
 *
 * Runs the shared `runStorageAdapterContract` kit over the real flat
 * `createDurableObjectStorageAdapter` routing (create/fork/delete cascade +
 * append/awaitChange forwarding + lineage edges) backed by an in-memory namespace
 * so it executes under vitest without `cloudflare:workers`.
 *
 * Scope note: this exercises adapter routing/logic, NOT workerd RPC structured
 * clone. Real-DO conformance (including the `AbortSignal` `DataCloneError`
 * boundary that motivated the seam redesign) is best-effort and deploy-gated —
 * see `@streamsy/conformance-tests` (`do.conformance` + the
 * `do.sse-final-append-diagnostic` regression test).
 */
import { describe, it } from "vitest";
import { runStorageAdapterContract } from "@streamsy/core";
import { createDurableObjectStorageAdapter } from "./adapter.ts";
import { createFakeNamespace } from "./testing/in-memory-namespace.ts";

describe("durable object adapter (in-memory namespace) — storage contract", () => {
  runStorageAdapterContract(
    () => createDurableObjectStorageAdapter({ namespace: createFakeNamespace().namespace }),
    { it },
  );
});
