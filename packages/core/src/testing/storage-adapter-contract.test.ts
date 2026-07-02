import { describe, it } from "vitest";
import type { StorageAdapter } from "../types/storage-adapter.ts";
import { createMemoryStorageAdapter } from "../storage/memory/adapter.ts";
import { runStorageAdapterContract } from "./storage-adapter-contract.ts";

describe("memory adapter — storage contract", () => {
  runStorageAdapterContract(() => createMemoryStorageAdapter(), { it });
});

describe("forkless minimal adapter — storage contract", () => {
  // Strip the optional `fork` so the kit drives the minimal-adapter floor that
  // does NOT implement fork. `fork` is capability-by-presence: omitting it is a
  // valid configuration whose protocol fork intents return `not-supported` —
  // there is no core fork fallback. This proves the kit holds a forkless adapter
  // to the rest of the contract and treats the absent capability deliberately
  // (fork/soft-delete cases are skipped because the capability is genuinely
  // unsupported, not silently assumed present).
  runStorageAdapterContract(
    (): StorageAdapter => {
      const base = createMemoryStorageAdapter();
      return { ...base, fork: undefined };
    },
    { it },
  );
});
