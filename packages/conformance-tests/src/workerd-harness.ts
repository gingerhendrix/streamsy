import { cpSync, existsSync, lstatSync, mkdirSync, realpathSync, rmSync } from "node:fs";
import { NodePath } from "@effect/platform-node";
import { Context, Effect, Layer, Path } from "effect";

const { basename, dirname, isAbsolute, relative, resolve, sep } = Context.get(
  Effect.runSync(Effect.scoped(Layer.build(NodePath.layer))),
  Path.Path,
);

export const isDescendant = (source: string, candidate: string): boolean => {
  const path = relative(source, candidate);

  return path !== "" && path !== ".." && !path.startsWith(`..${sep}`) && !isAbsolute(path);
};

const rejectSymlinkPath = (path: string): void => {
  let current = resolve(path);

  for (;;) {
    if (existsSync(current) && lstatSync(current).isSymbolicLink())
      throw new Error(`Retention path must not contain a symlink: ${current}`);
    const parent = dirname(current);

    if (parent === current) return;
    current = parent;
  }
};

export const retentionDestination = (root: string, configured?: string): string | undefined => {
  if (configured === undefined) return undefined;

  if (!isAbsolute(configured)) throw new Error("STREAMSY_WORKERD_RETENTION must be absolute");
  rejectSymlinkPath(root);
  const sourceReal = realpathSync(root);
  const retention = resolve(configured);
  rejectSymlinkPath(retention);

  if (retention === sourceReal || isDescendant(sourceReal, retention)) {
    throw new Error(
      "STREAMSY_WORKERD_RETENTION must not be the persistence root or its descendant",
    );
  }

  mkdirSync(retention, { recursive: true });
  rejectSymlinkPath(retention);
  const destination = resolve(retention, basename(root));
  rejectSymlinkPath(destination);

  const destinationReal = existsSync(destination)
    ? realpathSync(destination)
    : resolve(realpathSync(retention), basename(destination));

  if (
    destinationReal === sourceReal ||
    isDescendant(sourceReal, destinationReal) ||
    isDescendant(destinationReal, sourceReal)
  ) {
    throw new Error(
      "STREAMSY_WORKERD_RETENTION must not overlap the persistence root or its descendant",
    );
  }

  return destination;
};

export const reclaimRoot = (
  root: string,
  configured: string | undefined,
  removeOwned: (path: string) => void = (path) => rmSync(path, { recursive: true, force: true }),
): ReadonlyArray<unknown> => {
  const errors: Array<unknown> = [];

  try {
    const destination = retentionDestination(root, configured);

    if (destination !== undefined) {
      mkdirSync(resolve(destination, ".."), { recursive: true });
      cpSync(root, destination, { recursive: true });
    }
  } catch (error) {
    errors.push(error);
  }

  try {
    removeOwned(root);
  } catch (error) {
    errors.push(error);
  }

  return errors;
};

export interface WorkerdDisposable {
  readonly dispose: () => Promise<void>;
}

export interface WorkerdOwnedState<I extends WorkerdDisposable = WorkerdDisposable> {
  readonly root: string;
  instance?: I;
  disposed: boolean;
}

export interface WorkerdOwnedRegistry<I extends WorkerdDisposable = WorkerdDisposable> {
  readonly states: Set<WorkerdOwnedState<I>>;
}

export const createWorkerdOwnedRegistry = <
  I extends WorkerdDisposable = WorkerdDisposable,
>(): WorkerdOwnedRegistry<I> => ({
  states: new Set(),
});

export const registerWorkerdState = <I extends WorkerdDisposable>(
  registry: WorkerdOwnedRegistry<I>,
  root: string,
): WorkerdOwnedState<I> => {
  const state: WorkerdOwnedState<I> = { root, disposed: false };
  registry.states.add(state);

  return state;
};

export const cleanupWorkerdState = (
  state: WorkerdOwnedState,
  configuredRetention: string | undefined,
  removeOwned?: (path: string) => void,
): Promise<{ readonly done: boolean; readonly errors: ReadonlyArray<unknown> }> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const errors: Array<unknown> = [];

      if (!state.disposed && state.instance !== undefined) {
        const instance = state.instance;
        yield* Effect.promise(() => instance.dispose()).pipe(
          Effect.andThen(() =>
            Effect.sync(() => {
              state.disposed = true;
            }),
          ),
          Effect.catchDefect((error) =>
            Effect.sync(() => {
              errors.push(error);
            }),
          ),
        );
      } else if (state.instance === undefined) {
        state.disposed = true;
      }

      if (!state.disposed) {
        errors.push(new Error("Persistence root retained while Miniflare disposal is unresolved"));

        return { done: false, errors };
      }

      errors.push(...reclaimRoot(state.root, configuredRetention, removeOwned));

      return { done: !existsSync(state.root), errors };
    }),
  );

export const cleanupWorkerdRegistry = <I extends WorkerdDisposable>(
  registry: WorkerdOwnedRegistry<I>,
  configuredRetention: string | undefined,
  removeOwned?: (path: string) => void,
): Promise<ReadonlyArray<unknown>> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const errors: Array<unknown> = [];

      for (let attempt = 0; attempt < 2; attempt += 1) {
        for (const state of Array.from(registry.states)) {
          yield* Effect.promise(() =>
            cleanupWorkerdState(state, configuredRetention, removeOwned),
          ).pipe(
            Effect.andThen((result) =>
              Effect.sync(() => {
                errors.push(...result.errors);

                if (result.done) registry.states.delete(state);
              }),
            ),
            Effect.catchDefect((error) =>
              Effect.sync(() => {
                errors.push(error);
              }),
            ),
          );
        }
      }

      return errors;
    }),
  );

export interface WorkerdRunnerLifecycleOptions<I extends WorkerdDisposable> {
  readonly createRoot: () => string;
  readonly createInstance: (root: string) => I;
  readonly ready: (instance: I) => Promise<string>;
  readonly inspect?: (instance: I) => Promise<void>;
  readonly configuredRetention?: string;
  readonly removeOwned?: (path: string) => void;
}

export interface WorkerdRunnerLifecycle<I extends WorkerdDisposable> {
  readonly registry: WorkerdOwnedRegistry<I>;
  readonly beforeAll: () => Promise<string>;
  readonly afterAll: () => Promise<void>;
}

/** Registration precedes construction; unresolved disposal keeps the root owned. */
export const createWorkerdRunnerLifecycle = <I extends WorkerdDisposable>(
  options: WorkerdRunnerLifecycleOptions<I>,
): WorkerdRunnerLifecycle<I> => {
  const registry = createWorkerdOwnedRegistry<I>();

  const cleanupOne = (state: WorkerdOwnedState<I>, inspect: boolean) =>
    Effect.gen(function* () {
      const errors: Array<unknown> = [];
      const instance = state.instance;
      const inspectInstance = options.inspect;

      if (inspect && instance !== undefined && inspectInstance !== undefined) {
        yield* Effect.promise(() => inspectInstance(instance)).pipe(
          Effect.catchDefect((error) =>
            Effect.sync(() => {
              errors.push(error);
            }),
          ),
        );
      }

      yield* Effect.promise(() =>
        cleanupWorkerdState(state, options.configuredRetention, options.removeOwned),
      ).pipe(
        Effect.andThen((cleanup) =>
          Effect.sync(() => {
            errors.push(...cleanup.errors);

            if (cleanup.done) registry.states.delete(state);
          }),
        ),
        Effect.catchDefect((error) =>
          Effect.sync(() => {
            errors.push(error);
          }),
        ),
      );

      return errors;
    });

  const beforeAll = (): Promise<string> =>
    Effect.runPromise(
      Effect.gen(function* () {
        const root = options.createRoot();
        const state = registerWorkerdState(registry, root);

        return yield* Effect.gen(function* () {
          const instance = options.createInstance(root);
          state.instance = instance;

          return yield* Effect.promise(() => options.ready(instance));
        }).pipe(
          Effect.catchDefect((error) =>
            Effect.gen(function* () {
              const cleanupErrors = yield* cleanupOne(state, false);

              return yield* Effect.die(
                cleanupErrors.length > 0
                  ? new AggregateError([error, ...cleanupErrors], "Workerd runner startup failed", {
                      cause: error,
                    })
                  : error,
              );
            }),
          ),
        );
      }),
    );

  const afterAll = (): Promise<void> =>
    Effect.runPromise(
      Effect.gen(function* () {
        const errors: Array<unknown> = [];

        for (let attempt = 0; attempt < 2; attempt += 1) {
          for (const state of Array.from(registry.states)) {
            errors.push(...(yield* cleanupOne(state, true)));
          }
        }

        if (errors.length > 0)
          return yield* Effect.die(new AggregateError(errors, "Workerd runner cleanup failed"));

        return undefined;
      }),
    );

  return { registry, beforeAll, afterAll };
};
