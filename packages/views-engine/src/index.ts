export { coalesceChanges } from "./change.ts";
export { maintainGraph } from "./engine.ts";
export type {
  MaintainGraphInput,
  MaintainGraphResult,
  OperationCounts,
  SourceChanges,
} from "./engine.ts";
export { OperatorFault } from "./errors.ts";
export type { OperatorPhase } from "./errors.ts";
export { evaluate, evaluateOptional } from "./expression.ts";
export { asRowKey, canonicalJson, encodeRowKey } from "./key.ts";
export { fullRecompute, normalizeResult } from "./reference.ts";
export type { FullRecomputeInput, FullRecomputeResult } from "./reference.ts";
export { planRequirements } from "./requirements.ts";
export type {
  AccumulatorRequirement,
  ArrangementRequirement,
  OperatorRequirements,
} from "./requirements.ts";
export { emptyOperatorState } from "./state.ts";
export type * from "./state.ts";
