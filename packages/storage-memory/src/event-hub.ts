import type { StreamEventType, WaitForEventOptions, WaitForEventResult } from "@streamsy/core";
import type { MemoryStream } from "./stream.ts";

export class MemoryEventHub {
  constructor(private readonly stream: MemoryStream) {}

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.stream.waitForEvent(options);
  }

  notify(type: StreamEventType): void {
    this.stream.notify(type);
  }
}
