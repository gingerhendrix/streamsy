import { Effect, Semaphore } from "effect";
import type { InputMap } from "./batch.ts";
import { recordKey } from "./checkpoint.ts";
import type { Fused, Pinned } from "./projection.ts";
import type { RunOptions } from "./read.ts";
import { run, type Host, type Progress } from "./run.ts";
import type { ProjectionFault } from "./fault.ts";

/**
 * Process-scoped locks keyed exactly like checkpoint records. Entries are retained
 * for the life of the module so repeated triggers reuse the same semaphore.
 */
const locks = new Map<string, Semaphore.Semaphore>();

const lockFor = (projection: {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
}) => {
  const key = recordKey(projection);
  const existing = locks.get(key);
  if (existing !== undefined) return existing;
  const created = Semaphore.makeUnsafe(1);
  locks.set(key, created);
  return created;
};

/** Runs one projection at a time for this canonical key in the current process. */
export const serialized = <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Pinned<Inputs, O, E, R>,
  options: RunOptions = {},
): Effect.Effect<Progress, E | ProjectionFault, R | Host> =>
  Effect.suspend(() => lockFor(projection).withPermits(1)(run(projection, options)));
