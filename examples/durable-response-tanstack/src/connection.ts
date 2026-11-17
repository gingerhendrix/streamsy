import { durableStreamConnection } from "@durable-streams/tanstack-ai-transport";

/** Fresh native connection on each page mount: replay the complete raw event log. */
export const connection = (id: string, base = "") =>
  durableStreamConnection({
    sendUrl: `${base}/api/chat/${id}`,
    readUrl: `${base}/responses/${id}`,
    emitSnapshotOnSubscribe: false,
  });
