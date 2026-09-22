/**
 * Compile-time contract for `StreamRoute`.
 *
 * `Params` must be inferred from the codec record, and the template's `:name`
 * set must equal the codec record's key set. `bun test` does not collect this
 * file, so every assertion below is checked by `tsc` alone.
 */
import { Schema } from "effect";
import type { Params } from "../../src/toolkit/route.ts";
import * as StreamRoute from "../../src/toolkit/route.ts";

const Entry = Schema.Struct({ text: Schema.String });
const codecs = { user: Schema.String };
type Codecs = typeof codecs;

const inferred: Params<Codecs> = { user: "ann" };
void inferred;
// @ts-expect-error the parameter type comes from the codec, not from the caller
const wrongType: Params<Codecs> = { user: 7 };
void wrongType;
// @ts-expect-error the parameter set is exactly the codec record's key set
const wrongKey: Params<Codecs> = { account: "ann" };
void wrongKey;

const numbers = { orderId: Schema.NumberFromString };
const numeric: Params<typeof numbers> = { orderId: 7 };
void numeric;
// @ts-expect-error a numeric codec decodes to a number, not a string
const numericText: Params<typeof numbers> = { orderId: "7" };
void numericText;

const two = { user: Schema.String, kind: Schema.String };
const both: Params<typeof two> = { user: "ann", kind: "note" };
void both;

// A matching template and codec record is accepted.
StreamRoute.json("journal/:user", { params: { user: Schema.String }, schema: Entry });

const renamed = { account: Schema.String };
// @ts-expect-error the template names a parameter that the codec record omits
StreamRoute.json("journal/:user", { params: renamed, schema: Entry });

const extra = { user: Schema.String, extra: Schema.String };
// @ts-expect-error the codec record carries a parameter that the template omits
StreamRoute.json("journal/:user", { params: extra, schema: Entry });

const one = { user: Schema.String };
// @ts-expect-error a two-parameter template needs both codecs
StreamRoute.json("journal/:user/:kind", { params: one, schema: Entry });

StreamRoute.bytes("uploads/:name", { params: { name: Schema.String }, contentType: "image/png" });

// @ts-expect-error the bytes template names a parameter that the codec record omits
StreamRoute.bytes("uploads/:name", { params: renamed });

// A widened string template cannot be checked, so the declaration is accepted.
const widened: string = "journal/:user";
StreamRoute.json(widened, { params: one, schema: Entry });

// The decoded parameter type flows into `ref`, so a route builds its own ref shape.
const journal = StreamRoute.json("journal/:user", { params: one, schema: Entry });
const built = journal.ref({ user: "ann" });
void built;
// @ts-expect-error the ref constructor takes the decoded parameter set
journal.ref({ account: "ann" });

const catalog = StreamRoute.state("catalog/:user", {
  params: one,
  collections: { entry: { schema: Entry, key: "text" } },
});
void catalog.ref({ user: "ann" }).collections.entry.schema;
StreamRoute.state("catalog/:user", {
  // @ts-expect-error template params must be exact
  params: {},
  collections: { entry: { schema: Entry, key: "text" } },
});
StreamRoute.state("catalog/:user", {
  params: one,
  // @ts-expect-error collection keys must name a string field in the row
  collections: { entry: { schema: Entry, key: "missing" } },
});
