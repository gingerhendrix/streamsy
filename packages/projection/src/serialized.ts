import type { Named } from "./outputs.ts";
import { Effect, Semaphore } from "effect";
import type { InputMap } from "./batch.ts";
import { recordKey, type ProjectionKey } from "./checkpoint.ts";
import type { Fused, Pinned } from "./projection.ts";
import type { RunOptions } from "./read.ts";
import { run, type Host, type Progress } from "./run.ts";
import type { ProjectionFault } from "./fault.ts";

/** Users count both holders and waiters, preventing a forgotten key from splitting its queue. */
const locks = new Map<string, { semaphore: Semaphore.Semaphore; users: number; retire: boolean }>();

export const withPermit = <A, E, R>(
  projection: ProjectionKey,
  body: Effect.Effect<A, E, R>,
  retire = false,
): Effect.Effect<A, E, R> =>
  Effect.suspend(() => {
    const key = recordKey(projection);
    const entry = locks.get(key) ?? { semaphore: Semaphore.makeUnsafe(1), users: 0, retire: false };
    locks.set(key, entry);
    entry.users += 1;
    return entry.semaphore
      .withPermits(1)(
        body.pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              if (retire) entry.retire = true;
            }),
          ),
        ),
      )
      .pipe(
        Effect.ensuring(
          Effect.sync(() => {
            entry.users -= 1;
            if (entry.retire && entry.users === 0) locks.delete(key);
          }),
        ),
      );
  });

/** Runs one projection at a time for this canonical key across hosts in the current process. */
export const serialized = <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Pinned<Inputs, O, E, R> | Named<Inputs, E, R>,
  options: RunOptions = {},
): Effect.Effect<Progress, E | ProjectionFault, R | Host> =>
  withPermit(projection, run(projection, options));

/** Internal test probe for lifecycle assertions; not a package export. */
export const hasLock = (key: string): boolean => locks.has(key);
