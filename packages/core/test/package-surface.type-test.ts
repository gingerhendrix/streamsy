import type * as Root from "../src/index.ts";

// @ts-expect-error Reader is a protocol-tier type.
type Reader = Root.Reader;
// @ts-expect-error Writer is a protocol-tier type.
type Writer = Root.Writer;
// @ts-expect-error StreamRoute is a protocol-tier declaration.
type Route = Root.StreamRoute.StreamRoute<{}, string>;
export type RemovedRootTypes = [Reader, Writer, Route];
