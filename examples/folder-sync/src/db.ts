import {
  createStateSchema,
  isChangeEvent,
  isControlEvent,
  type StateEvent,
} from "@durable-streams/state";
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

/** State 0.3.2 truncates outside a TanStack sync transaction. Express the same
 * reset as deletes of source keys, then let StreamDB commit the suffix normally.
 * Track source keys so pending bootstrap rows and optimistic rows stay distinct.
 */
const resetBatch = (type: string) => {
  const keys = new Set<string>();

  return (batch: { items: ReadonlyArray<StateEvent> }) => {
    const items: StateEvent[] = [];

    for (const event of batch.items) {
      if (isControlEvent(event) && event.headers.control === "reset") {
        for (const key of keys) items.push({ type, key, headers: { operation: "delete" } });
        keys.clear();
      } else {
        // Keep prior events so their real txids are acknowledged by StreamDB.
        items.push(event);

        if (!isChangeEvent(event) || event.type !== type) continue;

        if (event.headers.operation === "delete") keys.delete(event.key);
        else keys.add(event.key);
      }
    }

    batch.items = items;
  };
};

/** The client treats 410 as terminal. Replay through its ordinary fetch boundary
 * and prepend a State reset so the existing StreamDB clears before the new rows.
 */
const replayGone = (fetcher: typeof globalThis.fetch): typeof globalThis.fetch =>
  Object.assign(
    // oxlint-disable-next-line effecttsgo/async-function -- StreamDB uses native fetch and Response at the browser boundary.
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const response = await fetcher(input, init);
      const request = new Request(input, init);
      const url = new URL(request.url);

      if (response.status !== 410 || url.searchParams.get("offset") === "-1") return response;
      await response.body?.cancel();
      url.searchParams.set("offset", "-1");
      url.searchParams.delete("live");
      url.searchParams.delete("cursor");
      const replay = await fetcher(new Request(url, request));

      if (!replay.ok) return replay;

      const items =
        replay.status === 204
          ? []
          : Schema.decodeUnknownSync(Schema.Array(Schema.Unknown))(await replay.json());

      const headers = new Headers(replay.headers);
      headers.delete("content-length");
      headers.delete("content-encoding");
      headers.delete("etag");
      headers.set("cache-control", "no-store");
      headers.set("content-type", "application/json");

      return new Response(JSON.stringify([{ headers: { control: "reset" } }, ...items]), {
        headers,
      });
    },
    { preconnect: fetcher.preconnect },
  );

export const createFolderDb = (options: DbOptions = {}) => {
  const transport = browserTransport(options);
  const connection = connectionFor(transport, options);

  const streamDb = createStreamDB({
    streamOptions: {
      url: new URL("/streams/folders", transport.origin).href,
      contentType: "application/json",
      warnOnHttp: false,
      fetch: replayGone(connection.fetch),
    },
    state: folderState,
    onBeforeBatch: resetBatch("folder"),
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
      fetch: replayGone(connection.fetch),
    },
    state: fileState,
    onBeforeBatch: resetBatch("file"),
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
