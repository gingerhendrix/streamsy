export class MemoryMutationCoordinator {
  private lock?: Promise<void>;

  async withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    while (this.lock) await this.lock;
    let release!: () => void;
    this.lock = new Promise<void>((resolve) => (release = resolve));
    try {
      return await fn();
    } finally {
      this.lock = undefined;
      release();
    }
  }
}
