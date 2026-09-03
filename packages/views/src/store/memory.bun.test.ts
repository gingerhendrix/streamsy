import { viewStoreConformance } from "./conformance.ts";
import { makeMemoryBacking, memoryService } from "./memory.ts";

const backing = () => {
  const state = makeMemoryBacking();
  const open = () =>
    Promise.resolve({ store: memoryService(state), restart: open, close: () => Promise.resolve() });
  return open();
};
viewStoreConformance("memory", backing);
