export class DurableObjectLock {
  private readonly chain = new Map<string, Promise<void>>();
  private readonly releasers = new Map<string, { key: string; release: () => void }>();

  async withLock<T>(fn: () => Promise<T>): Promise<T> {
    const token = await this.acquire("default");
    try {
      return await fn();
    } finally {
      await this.release(token);
    }
  }

  async acquire(key: string): Promise<string> {
    while (this.chain.has(key)) await this.chain.get(key);
    const token = `${key}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    let release!: () => void;
    this.chain.set(key, new Promise<void>((resolve) => (release = resolve)));
    this.releasers.set(token, { key, release });
    return token;
  }

  async release(token: string): Promise<void> {
    const entry = this.releasers.get(token);
    if (!entry) return;
    this.releasers.delete(token);
    this.chain.delete(entry.key);
    entry.release();
  }
}
