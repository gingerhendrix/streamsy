import { Serve } from "@streamsy/serve";
import { Schema } from "effect";
import { Files, FolderKey } from "./streams.ts";

/**
 * The router limits named parameters to 100 characters by default. A valid
 * FolderKey can contain 498 hex characters, so capture it with a wildcard.
 * Serve.mount still owns method rejection, State metadata and all stream reads.
 */
export const mountFiles = (path: `/${string}` = "/streams/*") =>
  Serve.mount(
    {
      paramSchema: Schema.Struct({ "*": Schema.String }),
      at: ({ "*": rest }) => {
        const match = /^folders\/([a-f0-9]+)\/files$/.exec(rest);

        if (match?.[1] === undefined) throw new Error("Invalid folder stream path");

        return Files.at({ folderId: Schema.decodeSync(FolderKey)(match[1]) });
      },
    },
    path,
  );
