/** In-process fallback lock provider used when adapters do not provide locks. */

export class InProcessLockProvider {
  private locks = new Map<string, Promise<void>>();

  async withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
    while (this.locks.has(key)) await this.locks.get(key);
    let release!: () => void;
    this.locks.set(key, new Promise<void>((resolve) => (release = resolve)));
    try {
      return await fn();
    } finally {
      this.locks.delete(key);
      release();
    }
  }
}
