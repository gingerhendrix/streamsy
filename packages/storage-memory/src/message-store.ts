import type { ListMessagesOptions, StoredMessage } from "@streamsy/core";
import type { MemoryStream } from "./stream.ts";
import { clone } from "./memory-entry.ts";

export class MemoryMessageStore {
  constructor(private readonly stream: MemoryStream) {}

  async appendMessages(messages: StoredMessage[]): Promise<void> {
    this.stream.mustEntry().messages.push(...clone(messages));
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const messages = this.stream.entry?.messages ?? [];
    let out = messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return clone(out);
  }

  async deleteMessages(): Promise<void> {
    if (this.stream.entry) this.stream.entry.messages = [];
  }
}
