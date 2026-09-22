/*
 * The parameter codecs are an open, author-declared record, so this module
 * bridges between that record and the decoded parameter set in two places:
 * `decodeParams` builds the bag one decoded value per codec key, and `buildId`
 * reads one already-decoded value back through its own codec. Both bridges carry
 * a stated invariant at the assertion site.
 */
/* oxlint-disable anti-slop/no-unsafe-dictionary-type, anti-slop/no-known-value-widening, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion, anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- This module is the checked boundary between an author's codec record and the decoded parameter set; every widening below is guarded by the invariant in its own comment. */
import { Option, Schema } from "effect";
import { StreamId } from "../schema/index.ts";
import * as StreamRef from "./ref.ts";
import { compileSegments, isCanonicalIdSegment, type Segment } from "./route-internal.ts";

/**
 * Inert id family. Constructing a route acquires no service and no resource.
 *
 * A route owns one id grammar and the ref constructor for its members. The same
 * value drives two host decisions: the binding table picks the storage backend
 * for an id, and `Placement.byRoute` picks the Durable Object that owns it.
 */
export interface StreamRoute<RouteParams, A, RD = never, RE = never> {
  readonly _tag: "StreamRoute";
  /** Relative template, for example `journal/:user`. Empty for a custom route. */
  readonly template: string;
  /** The router-facing contract. True when this route owns the id. */
  readonly match: (id: string) => boolean;
  /** The primitive. `None` when the id does not match or a parameter fails to decode. */
  readonly parse: (id: string) => Option.Option<RouteParams>;
  /** Build the member ref. `parse(ref(params).id)` must be `Some(params)`. */
  readonly ref: (params: RouteParams) => StreamRef.StreamRef<A, RD, RE>;
}

/** A route whose member refs retain their declared State collections. */
export type StateRoute<P, C extends StreamRef.Collections> = Omit<
  StreamRoute<
    P,
    StreamRef.CollectionsChange<C>,
    StreamRef.CollectionsDecodingServices<C>,
    StreamRef.CollectionsEncodingServices<C>
  >,
  "ref"
> & {
  readonly ref: (params: P) => StreamRef.StateRef<C>;
};

/** The minimal shape any consumer needs. The binding table reads only this. */
export interface Matcher {
  readonly match: (id: string) => boolean;
}

/** One codec per template parameter. Decoding runs from a string with no services. */
export type ParamCodecs = Readonly<Record<string, Schema.Codec<unknown, string, never, never>>>;

export type Params<Codecs extends ParamCodecs> = {
  readonly [Name in keyof Codecs]: Codecs[Name]["Type"];
};

type TemplateNames<Template extends string> =
  Template extends `${string}:${infer Name}/${infer Rest}`
    ? Name | TemplateNames<Rest>
    : Template extends `${string}:${infer Name}`
      ? Name
      : never;

/** Every `:name` must have a codec, and every codec must appear in the template. */
export type ExactTemplateParams<
  Template extends string,
  Codecs extends ParamCodecs,
> = string extends Template
  ? unknown
  : Exclude<TemplateNames<Template>, keyof Codecs> extends never
    ? Exclude<keyof Codecs, TemplateNames<Template>> extends never
      ? unknown
      : never
    : never;

function decodeParams<Codecs extends ParamCodecs>(
  codecs: Codecs,
  raw: Readonly<Record<string, string>>,
): Option.Option<Params<Codecs>> {
  const decoded: Record<string, unknown> = {};
  for (const name of Object.keys(codecs)) {
    const value = raw[name];
    const codec = codecs[name];
    if (value === undefined || codec === undefined) return Option.none();
    // A parameter codec decodes from a string with no services, so this call is
    // synchronous, pure and total.
    const result = Schema.decodeUnknownOption(codec)(value);
    if (Option.isNone(result)) return Option.none();
    decoded[name] = result.value;
  }
  // The loop above visits exactly the keys of `Codecs` and writes each decoded
  // value under its own name, so the bag has the shape of `Params<Codecs>`.
  return Option.some(decoded as Params<Codecs>);
}

function matchTemplate<Codecs extends ParamCodecs>(
  segments: ReadonlyArray<Segment>,
  codecs: Codecs,
  id: string,
): Option.Option<Params<Codecs>> {
  const actual = id.split("/");
  if (actual.length !== segments.length) return Option.none();
  const raw: Record<string, string> = {};
  for (const [index, segment] of segments.entries()) {
    const value = actual[index];
    if (value === undefined) return Option.none();
    if (segment.kind === "literal") {
      if (value !== segment.value) return Option.none();
      continue;
    }
    if (!isCanonicalIdSegment(value)) return Option.none();
    raw[segment.name] = value;
  }
  return decodeParams(codecs, raw);
}

function buildId<Codecs extends ParamCodecs>(
  template: string,
  segments: ReadonlyArray<Segment>,
  codecs: Codecs,
  params: Params<Codecs>,
): StreamId {
  const record: Readonly<Record<string, unknown>> = params;
  const built = segments.map((segment) => {
    if (segment.kind === "literal") return segment.value;
    const codec = codecs[segment.name];
    const value = record[segment.name];
    if (codec === undefined || value === undefined)
      throw new RangeError(`stream route ${template} has no value for parameter: ${segment.name}`);
    // The parameter bag stores each entry as `Codecs[Name]["Type"]`, so this
    // value is exactly the decoded type that this parameter's codec encodes.
    const text = Option.getOrUndefined(Schema.encodeOption(codec)(value));
    if (text === undefined)
      throw new RangeError(
        `stream route ${template} parameter ${segment.name} cannot encode to an id segment`,
      );
    if (!isCanonicalIdSegment(text))
      throw new RangeError(
        `stream route ${template} parameter ${segment.name} is not a canonical id segment: ${text}`,
      );
    return text;
  });
  return StreamId.make(built.join("/"));
}

/**
 * The codec record and the template must name the same parameters. The type
 * check covers the common case; this backstop covers a widened template.
 */
function checkTemplateParams(
  template: string,
  names: ReadonlyArray<string>,
  codecs: ParamCodecs,
): void {
  for (const name of names)
    if (codecs[name] === undefined)
      throw new RangeError(`stream route ${template} has no codec for parameter: ${name}`);
  for (const name of Object.keys(codecs))
    if (!names.includes(name))
      throw new RangeError(`stream route codec is absent from the template: ${name}`);
}

export function json<const Template extends string, Codecs extends ParamCodecs, A, I, RD, RE>(
  template: Template,
  options: {
    readonly params: Codecs & ExactTemplateParams<Template, Codecs>;
    readonly schema: Schema.Codec<A, I, RD, RE>;
  },
): StreamRoute<Params<Codecs>, A, RD, RE> {
  const { segments, names } = compileSegments(template);
  const codecs = options.params;
  checkTemplateParams(template, names, codecs);
  return {
    _tag: "StreamRoute",
    template,
    match: (id) => Option.isSome(matchTemplate(segments, codecs, id)),
    parse: (id) => matchTemplate(segments, codecs, id),
    ref: (params) => StreamRef.json(buildId(template, segments, codecs, params), options),
  };
}

export function state<
  const Template extends string,
  Codecs extends ParamCodecs,
  const C extends StreamRef.Collections,
>(
  template: Template,
  options: {
    readonly params: Codecs & ExactTemplateParams<Template, Codecs>;
    readonly collections: C & StreamRef.ValidCollections<C>;
  },
): StateRoute<Params<Codecs>, C> {
  const { segments, names } = compileSegments(template);
  const codecs = options.params;
  checkTemplateParams(template, names, codecs);
  return {
    _tag: "StreamRoute",
    template,
    match: (id) => Option.isSome(matchTemplate(segments, codecs, id)),
    parse: (id) => matchTemplate(segments, codecs, id),
    ref: (params) => StreamRef.state(buildId(template, segments, codecs, params), options),
  };
}

export function bytes<const Template extends string, Codecs extends ParamCodecs>(
  template: Template,
  options: {
    readonly params: Codecs & ExactTemplateParams<Template, Codecs>;
    readonly contentType?: string;
  },
): StreamRoute<Params<Codecs>, Uint8Array> {
  const { segments, names } = compileSegments(template);
  const codecs = options.params;
  checkTemplateParams(template, names, codecs);
  return {
    _tag: "StreamRoute",
    template,
    match: (id) => Option.isSome(matchTemplate(segments, codecs, id)),
    parse: (id) => matchTemplate(segments, codecs, id),
    ref: (params) =>
      StreamRef.bytes(buildId(template, segments, codecs, params), {
        contentType: options.contentType ?? "application/octet-stream",
      }),
  };
}

/** Escape hatch for a grammar a template cannot express, such as a regex family. */
export function custom<RouteParams, A, RD, RE>(options: {
  readonly parse: (id: string) => Option.Option<RouteParams>;
  readonly ref: (params: RouteParams) => StreamRef.StreamRef<A, RD, RE>;
}): StreamRoute<RouteParams, A, RD, RE> {
  return {
    _tag: "StreamRoute",
    template: "",
    match: (id) => Option.isSome(options.parse(id)),
    parse: options.parse,
    ref: options.ref,
  };
}
