import { StorageContract } from "../../../src/testing/storage-contract.ts";
import { layer } from "../../../src/storage/memory/layer.ts";
import type { StorageCapabilities } from "../../../src/storage/capabilities.ts";
const normal: StorageCapabilities = {
  fork: "chain",
  atomicScope: "store",
  wake: "push",
  expiryIndex: "indexed",
};
const constrained: StorageCapabilities = {
  fork: "copy",
  atomicScope: "stream",
  wake: "poll",
  expiryIndex: "lazy",
};
StorageContract.run({
  name: "Memory chain / store / push / indexed",
  layer: layer(),
  expected: normal,
});
StorageContract.run({
  name: "Memory copy / stream / poll / lazy",
  layer: layer({ constrained: true }),
  expected: constrained,
});
StorageContract.assertModes([normal, constrained]);
