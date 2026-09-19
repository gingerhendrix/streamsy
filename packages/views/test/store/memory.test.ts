import { viewStoreConformance } from "@streamsy/views/store/conformance";
import { makeMemoryBacking, memoryService } from "@streamsy/views/store";

const backing = () => {
  const state = makeMemoryBacking();
  const open = () =>
    Promise.resolve({ store: memoryService(state), restart: open, close: () => Promise.resolve() });
  return open();
};
viewStoreConformance("memory", backing);
