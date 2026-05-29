export class MemoryExpiryScheduler {
  private timer?: ReturnType<typeof setTimeout>;

  scheduleExpiry(at: number, callback?: () => Promise<void>): void {
    void this.cancelExpiry();
    if (!callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timer = setTimeout(() => void callback(), delay);
  }

  async cancelExpiry(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }
}
