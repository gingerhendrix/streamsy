export class TimeoutScheduler {
  private timer?: ReturnType<typeof setTimeout>;

  schedule(at: number, callback?: () => Promise<void>): void {
    this.cancel();
    if (!callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timer = setTimeout(() => void callback(), delay);
  }

  cancel(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }
}
