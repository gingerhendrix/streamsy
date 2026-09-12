import { Context, Effect, Layer } from "effect";
import type { StreamsFault } from "../fault.ts";
import { StreamsReader, StreamsWriter, type Reader, type Writer } from "../protocol/tags.ts";
import type { Matcher } from "./route.ts";
import { templatesOverlap } from "./route-internal.ts";

/** The two services a backend carries, re-tagged under one Context key. */
export interface BackendServices {
  readonly reader: Reader<StreamsFault>;
  readonly writer: Writer<StreamsFault>;
}

/**
 * The Context key identifier for one backend name.
 *
 * The key carries this tag rather than the `Backend` value itself, so a named
 * backend stays comparable to its structural form and a binding table keeps
 * every name it was declared with.
 */
export interface BackendTag<Name extends string> {
  readonly _tag: "StreamsyBackend";
  readonly name: Name;
}

/**
 * One named storage backend for the routed Layer.
 *
 * A backend is one `Context` key. `layer(graph)` re-tags a complete protocol
 * graph under that key without building the graph, so two backends can share
 * one storage Layer value and a sibling Layer merges in either order.
 */
export interface Backend<Name extends string> {
  readonly name: Name;
  /** One key per backend name, so several protocol graphs coexist in one Context. */
  readonly key: Context.Key<BackendTag<Name>, BackendServices>;
  /** Re-tag one complete protocol graph under this key. It never builds the graph. */
  readonly layer: <E, R>(
    graph: Layer.Layer<StreamsReader | StreamsWriter, E, R>,
  ) => Layer.Layer<BackendTag<Name>, E, R>;
  /** Bind stream families to this backend for the routed Layer. */
  readonly serves: (...routes: ReadonlyArray<Matcher>) => Binding<Name>;
}

/**
 * The structural shape `layerRouted` reads. A named backend is not assignable to
 * another name, so the routed Layer takes this shape as its constraint and
 * derives the exact requirement from the literal bindings the caller passes.
 */
export interface AnyBackend {
  readonly key: Context.Key<{ readonly name: string }, BackendServices>;
}

export interface AnyBinding {
  readonly backend: AnyBackend;
  readonly routes: ReadonlyArray<Matcher>;
}

export interface Binding<Name extends string> {
  readonly backend: Backend<Name>;
  readonly routes: ReadonlyArray<Matcher>;
}

/**
 * The template a route carries, or `undefined` for a custom route.
 *
 * A `StreamRoute` always carries a `template` string and a custom route carries
 * the empty string, so the empty check is the whole test.
 */
const templateOf = (route: Matcher): string | undefined => {
  if (!("template" in route)) return undefined;
  // SAFETY: every `StreamRoute` carries `template` as a string, and a custom
  // route carries the empty string, so the narrowing above is complete.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- The invariant is stated in the comment above.
  const template = route.template as string;
  return template.length > 0 ? template : undefined;
};

/**
 * Two template routes in one binding table that can match the same id are a
 * composition defect: the id would have no single owner. A custom route carries
 * no template, so it is ordered instead and the first match wins.
 */
export function checkNoOverlap(bindings: ReadonlyArray<AnyBinding>): void {
  const templates: Array<string> = [];
  for (const binding of bindings)
    for (const route of binding.routes) {
      const template = templateOf(route);
      if (template !== undefined) templates.push(template);
    }
  for (const [index, template] of templates.entries())
    for (const other of templates.slice(index + 1))
      if (templatesOverlap(template, other))
        throw new RangeError(`stream route templates overlap: ${template} and ${other}`);
}

export const make = <Name extends string>(name: Name): Backend<Name> => {
  const key = Context.Service<BackendTag<Name>, BackendServices>(`@streamsy/core/Backend/${name}`);
  const backend: Backend<Name> = {
    name,
    key,
    layer: (graph) =>
      Layer.effect(key, Effect.all({ reader: StreamsReader, writer: StreamsWriter })).pipe(
        Layer.provide(graph),
      ),
    // `serves` runs after construction, so the binding carries this backend.
    serves: (...routes) => ({ backend, routes }),
  };
  return backend;
};
