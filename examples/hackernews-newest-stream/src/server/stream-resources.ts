import { StreamFamily, StreamRef } from "@streamsy/core";
import { HackerNewsStateChange } from "../state-schema.ts";
import { Option } from "effect";
import { sourceStreamId, targetStreamId } from "./config.ts";
import { HackerNewsSourceChange } from "./source-change.ts";

/** Configured ids are literal identities, including colon-prefixed segments. */
export class HackerNewsSource extends StreamFamily.custom<HackerNewsSource>()("HackerNews/Source", {
  parse: (id: string) => (id === sourceStreamId ? Option.some({}) : Option.none()),
  ref: () => StreamRef.json(sourceStreamId, { schema: HackerNewsSourceChange }),
}) {}
export class HackerNewsTarget extends StreamFamily.custom<HackerNewsTarget>()("HackerNews/Target", {
  parse: (id: string) => (id === targetStreamId ? Option.some({}) : Option.none()),
  ref: () => StreamRef.json(targetStreamId, { schema: HackerNewsStateChange }),
}) {}
/** The application creates the input; the projection owns output creation. */
export const hackerNewsSource = HackerNewsSource.at({});
