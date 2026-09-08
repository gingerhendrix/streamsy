import { cpSync, existsSync, lstatSync, mkdirSync, realpathSync, rmSync } from "node:fs";
import { basename, dirname, isAbsolute, relative, resolve, sep } from "node:path";

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

export const cleanupWorkerdState = async (
  state: WorkerdOwnedState,
  configuredRetention: string | undefined,
  removeOwned?: (path: string) => void,
): Promise<{ readonly done: boolean; readonly errors: ReadonlyArray<unknown> }> => {
  const errors: Array<unknown> = [];
  if (!state.disposed && state.instance !== undefined) {
    try {
      await state.instance.dispose();
      state.disposed = true;
    } catch (error) {
      errors.push(error);
    }
  } else if (state.instance === undefined) {
    state.disposed = true;
  }
  if (!state.disposed) {
    errors.push(new Error("Persistence root retained while Miniflare disposal is unresolved"));
    return { done: false, errors };
  }
  const rootErrors = reclaimRoot(state.root, configuredRetention, removeOwned);
  errors.push(...rootErrors);
  const rootGone = !existsSync(state.root);
  return { done: rootGone, errors };
};

export const cleanupWorkerdRegistry = async <I extends WorkerdDisposable>(
  registry: WorkerdOwnedRegistry<I>,
  configuredRetention: string | undefined,
  removeOwned?: (path: string) => void,
): Promise<ReadonlyArray<unknown>> => {
  const errors: Array<unknown> = [];
  for (let attempt = 0; attempt < 2; attempt += 1) {
    for (const state of Array.from(registry.states)) {
      try {
        const result = await cleanupWorkerdState(state, configuredRetention, removeOwned);
        errors.push(...result.errors);
        if (result.done) registry.states.delete(state);
      } catch (error) {
        errors.push(error);
      }
    }
  }
  return errors;
};

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

/**
 * Shared lifecycle used by the real Vitest hooks and by deterministic ownership tests.
 * Registration precedes construction, and an unresolved instance keeps its root owned.
 */
export const createWorkerdRunnerLifecycle = <I extends WorkerdDisposable>(
  options: WorkerdRunnerLifecycleOptions<I>,
): WorkerdRunnerLifecycle<I> => {
  const registry = createWorkerdOwnedRegistry<I>();

  const cleanupOne = async (
    state: WorkerdOwnedState<I>,
    inspect: boolean,
  ): Promise<ReadonlyArray<unknown>> => {
    const errors: Array<unknown> = [];
    if (inspect && state.instance !== undefined && options.inspect !== undefined) {
      try {
        await options.inspect(state.instance);
      } catch (error) {
        errors.push(error);
      }
    }
    try {
      const cleanup = await cleanupWorkerdState(
        state,
        options.configuredRetention,
        options.removeOwned,
      );
      errors.push(...cleanup.errors);
      if (cleanup.done) registry.states.delete(state);
    } catch (error) {
      errors.push(error);
    }
    return errors;
  };

  const beforeAll = async (): Promise<string> => {
    const root = options.createRoot();
    const state = registerWorkerdState(registry, root);
    try {
      const instance = options.createInstance(root);
      state.instance = instance;
      return await options.ready(instance);
    } catch (error) {
      const cleanupErrors = await cleanupOne(state, false);
      if (cleanupErrors.length > 0) {
        // oxlint-disable-next-line eslint(preserve-caught-error) -- Preserve startup and cleanup causes together.
        throw new AggregateError([error, ...cleanupErrors], "Workerd runner startup failed", {
          cause: error,
        });
      }
      throw error;
    }
  };

  const afterAll = async (): Promise<void> => {
    const errors: Array<unknown> = [];
    for (let attempt = 0; attempt < 2; attempt += 1) {
      for (const state of Array.from(registry.states)) {
        errors.push(...(await cleanupOne(state, true)));
      }
    }
    if (errors.length > 0) throw new AggregateError(errors, "Workerd runner cleanup failed");
  };

  return { registry, beforeAll, afterAll };
};
