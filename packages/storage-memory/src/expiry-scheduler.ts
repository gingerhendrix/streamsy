import type { MemoryStream } from "./stream.ts";

export class MemoryExpiryScheduler {
  constructor(private readonly stream: MemoryStream) {}

  scheduleExpiry(at: number, callback?: () => Promise<void>): void {
    this.stream.scheduleExpiry(at, callback);
  }

  cancelExpiry(): Promise<void> {
    return this.stream.cancelExpiry();
  }
}
