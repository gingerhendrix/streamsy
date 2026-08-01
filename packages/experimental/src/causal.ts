export {
  decodeStreamIdentity,
  encodeStreamIdentity,
  streamIdentity,
  streamIdentityEquals,
} from "./causal/identity.ts";
export {
  compareStreamPositions,
  coverage,
  sourceAck,
  sourceWatermark,
  streamPosition,
} from "./causal/coverage.ts";
export type { StreamIdentity } from "./causal/identity.ts";
export type { Coverage, SourceAck, SourceWatermark, StreamPosition } from "./causal/coverage.ts";
