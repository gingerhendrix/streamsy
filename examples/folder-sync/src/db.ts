import { createStateSchema } from "@durable-streams/state";
import { createStreamDB } from "@durable-streams/state/db";
import { Schema } from "effect";
import { File, type Head } from "../server/domain.ts";
import { Folder } from "../server/streams.ts";
import {
  browserTransport,
  folderStreamUrl,
  saveFile,
  textContent,
  type Transport,
} from "./client.ts";
import { trackConnection, type RetryEvents } from "./connection.ts";

export const folderState = createStateSchema({
  folders: { schema: Schema.toStandardSchemaV1(Folder), type: "folder", primaryKey: "folderId" },
});

export const fileState = createStateSchema({
  files: { schema: Schema.toStandardSchemaV1(File), type: "file", primaryKey: "path" },
});

interface DbOptions extends Partial<Transport> {
  events?: RetryEvents;
}

const connectionFor = (transport: Transport, options: DbOptions) =>
  trackConnection(
    transport.fetch,
    options.events ??
      (typeof window === "undefined"
        ? undefined
        : {
            online: window,
            visibility: document,
            isVisible: () => document.visibilityState === "visible",
          }),
  );

export const createFolderDb = (options: DbOptions = {}) => {
  const transport = browserTransport(options);
  const connection = connectionFor(transport, options);

  const streamDb = createStreamDB({
    streamOptions: {
      url: new URL("/streams/folders", transport.origin).href,
      contentType: "application/json",
      warnOnHttp: false,
      fetch: connection.fetch,
    },
    state: folderState,
  });

  return Object.assign(streamDb, { connection });
};

export interface EditFile {
  file: File;
  base: number;
  txid: string;
  bytes: Uint8Array;
  onAccepted?: (head: Head) => void;
}

export const createFileDb = (folderId: string, options: DbOptions = {}) => {
  const transport = browserTransport(options);
  const connection = connectionFor(transport, options);

  const streamDb = createStreamDB({
    streamOptions: {
      url: new URL(folderStreamUrl(folderId), transport.origin).href,
      contentType: "application/json",
      warnOnHttp: false,
      fetch: connection.fetch,
    },
    state: fileState,
    actions: ({ db }) => ({
      edit: {
        onMutate: ({ file }: EditFile) => {
          // Replace the whole row: TanStack partial updates represent deleted optional fields
          // as undefined, which an exact optional State field correctly rejects.
          if (db.collections.files.has(file.path)) db.collections.files.delete(file.path);
          db.collections.files.insert(file);
        },
        // oxlint-disable-next-line effecttsgo/async-function -- TanStack rolls back the optimistic row when this mutation promise rejects.
        mutationFn: async ({ file, base, txid, bytes, onAccepted }: EditFile) => {
          const head = await saveFile(folderId, file, base, txid, transport, bytes);
          await db.utils.awaitTxId(head.txid);
          onAccepted?.(head);
        },
      },
    }),
  });

  // Durable State upserts replace the entire value. TanStack defaults to partial merges,
  // which would retain old inline text when a later value refers to a blob.
  streamDb.collections.files.config.sync.rowUpdateMode = "full";

  return Object.assign(streamDb, { connection });
};

export type FolderDb = ReturnType<typeof createFolderDb>;

export type FileDb = ReturnType<typeof createFileDb>;

/** Hash before opening an optimistic transaction, so its local row is complete. */
export const editFile = (db: FileDb, current: File, text: string, author: string, txid: string) =>
  textContent(text).then((content) => {
    let accepted: Head | undefined;

    const transaction = db.actions.edit({
      file: {
        path: current.path,
        version: current.version,
        author: author.trim() || "anonymous",
        ...content,
      },
      base: current.version,
      txid,
      bytes: new TextEncoder().encode(text),
      onAccepted: (head: Head) => {
        accepted = head;
      },
    });

    return transaction.isPersisted.promise.then(() => {
      if (accepted === undefined) throw new Error("The server did not acknowledge the save");

      // Keep this exact accepted version even if another stream change has already arrived.
      return { ...accepted, size: content.size, inline: content.text !== undefined };
    });
  });
