import { Streamsy } from "@streamsy/core";
import { ObjectLink, Placement } from "@streamsy/serve/cloudflare";
import { Files, FolderWake } from "../server/streams.ts";
import { placement } from "./placement.ts";

/** Folder objects append wake rows to the index object, which owns the wake stream. */
export const wakeLink = ObjectLink.make({
  group: Streamsy.group(FolderWake),
  prefix: "/__wake",
  placement: Placement.byKey(() => "index"),
  // The index object creates the one wake stream at start.
  access: "append",
});

/** Internal protocol reads preserve missing/gone outcomes for the projection kernel. */
export const filesLink = ObjectLink.make({
  group: Streamsy.group(Files),
  prefix: "/__files",
  placement,
  access: "read",
});
