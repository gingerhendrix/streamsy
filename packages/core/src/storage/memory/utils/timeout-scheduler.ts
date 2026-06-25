export class TimeoutScheduler {
  private timer?: ReturnType<typeof setTimeout>;

  constructor(private readonly callback?: () => Promise<void> | void) {}

  schedule(at: number): void {
    this.cancel();
    if (!this.callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timer = setTimeout(() => {
      void Promise.resolve(this.callback?.()).catch((error) => {
        console.error("memory expiry callback failed", error);
      });
    }, delay);
  }

  cancel(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }
}
