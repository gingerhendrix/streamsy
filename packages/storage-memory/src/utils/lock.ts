export class MemoryLock {
  private lock?: Promise<void>;

  async withLock<T>(fn: () => Promise<T>): Promise<T> {
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
