import { createStateSchema } from "@durable-streams/state";
import { createStreamDB } from "@durable-streams/state/db";
import { Schema } from "effect";
import { ManifestFile } from "../server/streams.ts";
import { trackConnection } from "./connection.ts";

export const manifestState = createStateSchema({
  files: { schema: Schema.toStandardSchemaV1(ManifestFile), type: "file", primaryKey: "id" },
});

export const createFileDb = () => {
  const connection = trackConnection(globalThis.fetch, {
    online: window,
    visibility: document,
    isVisible: () => document.visibilityState === "visible",
  });

  const db = createStreamDB({
    streamOptions: {
      url: new URL("/streams/manifest", window.location.origin).href,
      contentType: "application/json",
      warnOnHttp: false,
      fetch: connection.fetch,
    },
    state: manifestState,
  });

  return Object.assign(db, { connection });
};

export type FileDb = ReturnType<typeof createFileDb>;
