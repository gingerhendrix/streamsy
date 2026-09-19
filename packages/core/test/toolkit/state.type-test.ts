/** Compile-time contract for Durable State key fields. */
import { Schema } from "effect";
import * as StreamRef from "../../src/toolkit/ref.ts";

const Entry = Schema.Struct({
  id: Schema.String,
  metadata: Schema.Struct({ source: Schema.String }),
});

StreamRef.state("entries", { schema: Entry, type: "entry", key: "id" });

// @ts-expect-error state keys must decode to strings or numbers
StreamRef.state("entries", { schema: Entry, type: "entry", key: "metadata" });
