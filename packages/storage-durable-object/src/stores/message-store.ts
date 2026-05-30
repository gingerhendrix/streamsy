import type { ListMessagesOptions, StoredMessage } from "@streamsy/core";
import { MESSAGE_PREFIX, messageKey } from "../lib/keys.ts";
import type { RecordStore } from "./record-store.ts";

type DurableObjectKv = DurableObjectStorage["kv"];

export class MessageStore {
  constructor(
    private readonly records: RecordStore,
    private readonly kv: DurableObjectKv,
  ) {}

  async appendMessages(messages: StoredMessage[]): Promise<void> {
    await this.records.requireRecord();
    for (const msg of messages) this.kv.put(messageKey(msg.offset), msg);
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const listOptions: DurableObjectListOptions = { prefix: MESSAGE_PREFIX };
    if (options.after) listOptions.startAfter = messageKey(options.after);
    const entries = this.kv.list<StoredMessage>(listOptions);
    const messages: StoredMessage[] = [];
    for (const [, value] of entries) {
      if (options.until && value.offset > options.until) break;
      messages.push(value);
      if (options.limit !== undefined && messages.length >= options.limit) break;
    }
    return messages;
  }

  async deleteMessages(): Promise<void> {
    const entries = this.kv.list({ prefix: MESSAGE_PREFIX });
    for (const [key] of entries) this.kv.delete(key);
  }
}
