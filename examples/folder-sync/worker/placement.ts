import { Placement, rule } from "@streamsy/serve/cloudflare";
import { Files } from "../server/streams.ts";
import type { Env } from "./env.ts";

/** Placement decodes the hex URL segment. Object names use the opaque folder id. */
export const placement = Placement.byRoute([
  rule({ stream: Files, owner: ({ folderId }) => folderId }),
]);

export const folderOwner = (folderId: string) => placement.name(Files.at({ folderId }).id);

/** The Durable Object that owns one folder's `Files` stream and its writes. */
export const folderObject = (env: Env, folderId: string) =>
  env.FOLDERS.get(env.FOLDERS.idFromName(folderOwner(folderId)));

/** The one Durable Object that owns the registry and the search index. */
export const indexObject = (env: Env) => env.INDEX.get(env.INDEX.idFromName("index"));
