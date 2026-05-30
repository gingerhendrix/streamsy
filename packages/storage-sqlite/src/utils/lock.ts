/**
 * In-process per-stream mutation lock.
 *
 * Serializes mutation callbacks for one stream within a single process. Because
 * the factory caches one stream object per id, all concurrent requests for the
 * same id share this lock and run their `withMutationLock` callbacks in order.
 * For multi-process access to a shared file the SQLite database itself remains
 * the cross-process correctness boundary.
 */
export class StreamLock {
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
