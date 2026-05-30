import type { ListMessagesOptions, StoredMessage } from "@streamsy/core";
import { clone } from "#lib/clone";
import type { RecordStore } from "./record-store.ts";

export class MessageStore {
  private messages: StoredMessage[] = [];

  constructor(private readonly records: RecordStore) {}

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
