import { StreamRef } from "@streamsy/core";
import { sourceStreamId } from "./config.ts";
import { HackerNewsSourceChange } from "./source-change.ts";

/** The application creates the input; the projection owns output creation. */
export const hackerNewsSource = StreamRef.json(sourceStreamId, { schema: HackerNewsSourceChange });
