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
