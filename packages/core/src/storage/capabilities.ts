import { Schema } from "effect";
export const StorageCapabilities = Schema.Struct({
  fork: Schema.Literals(["none", "copy", "chain"]),
  atomicScope: Schema.Literals(["stream", "store"]),
  wake: Schema.Literals(["poll", "push"]),
  expiryIndex: Schema.Literals(["lazy", "indexed"]),
});
export interface StorageCapabilities extends Schema.Schema.Type<typeof StorageCapabilities> {}
