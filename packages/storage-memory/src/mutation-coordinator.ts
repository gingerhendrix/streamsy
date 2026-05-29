import type { MemoryStream } from "./stream.ts";

export class MemoryMutationCoordinator {
  constructor(private readonly stream: MemoryStream) {}

  withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    return this.stream.withMutationLock(fn);
  }
}
