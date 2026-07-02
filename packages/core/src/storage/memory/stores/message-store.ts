import type { ListMessagesOptions, StoredMessage } from "../../../types/storage.ts";
import { clone } from "../lib/clone.ts";
import type { RecordStore } from "./record-store.ts";

export class MessageStore {
  private messages: StoredMessage[] = [];

  constructor(private readonly records: RecordStore) {}

  appendMessages(messages: StoredMessage[]): void {
    this.records.requireRecord();
    this.messages.push(...clone(messages));
  }

  listMessages(options: ListMessagesOptions = {}): StoredMessage[] {
    let out = this.messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return clone(out);
  }

  deleteMessages(): void {
    this.messages = [];
  }
}
