import type { ListMessagesOptions, StoredMessage } from "@streamsy/core";
import { clone } from "./clone.ts";
import type { MemoryRecordStore } from "./record-store.ts";

export class MemoryMessageStore {
  private messages: StoredMessage[] = [];

  constructor(private readonly records: MemoryRecordStore) {}

  async appendMessages(messages: StoredMessage[]): Promise<void> {
    this.records.requireRecord();
    this.messages.push(...clone(messages));
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    let out = this.messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return clone(out);
  }

  async deleteMessages(): Promise<void> {
    this.messages = [];
  }
}
