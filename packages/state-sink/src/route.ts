export interface SinkParamCodec<Value extends string = string> {
  readonly decode: (value: string) => Value;
}

export type SinkParamCodecs = Readonly<Record<string, SinkParamCodec>>;
export type DecodedSinkParams<Codecs extends SinkParamCodecs> = {
  readonly [Name in keyof Codecs]: ReturnType<Codecs[Name]["decode"]>;
};

export interface CompiledSinkRoute<Codecs extends SinkParamCodecs> {
  readonly template: string;
  readonly parameterNames: readonly (keyof Codecs & string)[];
  readonly build: (params: DecodedSinkParams<Codecs>) => string;
  readonly match: (pathname: string) => SinkRouteMatch<DecodedSinkParams<Codecs>>;
}

export type SinkRouteMatch<Params> =
  | { readonly kind: "matched"; readonly params: Params }
  | { readonly kind: "mismatch" }
  | { readonly kind: "invalid"; readonly parameter: string; readonly detail: string };

type Segment =
  | { readonly kind: "literal"; readonly value: string }
  | { readonly kind: "parameter"; readonly name: string; readonly codec: SinkParamCodec };

export function compileSinkRoute<Codecs extends SinkParamCodecs>(
  template: string,
  codecs: Codecs,
): CompiledSinkRoute<Codecs> {
  const parts = splitTemplate(template);
  const seen = new Set<string>();
  const segments: Segment[] = parts.map((part) => {
    if (!part.startsWith(":")) return { kind: "literal", value: part };
    const name = part.slice(1);
    if (name.length === 0) throw new Error("sink route parameter names cannot be empty");
    if (seen.has(name)) throw new Error(`duplicate sink route parameter: ${name}`);
    seen.add(name);
    const codec = codecs[name];
    if (codec === undefined) throw new Error(`sink route parameter has no codec: ${name}`);
    return { kind: "parameter", name, codec };
  });

  for (const name of Object.keys(codecs)) {
    if (!seen.has(name)) throw new Error(`sink parameter is absent from its route: ${name}`);
  }

  // SAFETY: every selected segment is a parameter whose name was checked
  // against `codecs` while the route was compiled.
  const parameterNames = segments
    .filter(
      (segment): segment is Extract<Segment, { readonly kind: "parameter" }> =>
        segment.kind === "parameter",
    )
    .map((segment) => segment.name) as (keyof Codecs & string)[];

  const compiled: CompiledSinkRoute<Codecs> = {
    template,
    parameterNames: Object.freeze(parameterNames),
    build: (params: DecodedSinkParams<Codecs>) =>
      `/${segments
        .map((segment) => {
          if (segment.kind === "literal") return segment.value;
          const value = params[segment.name];
          if (value === undefined) throw new Error(`missing sink route parameter: ${segment.name}`);
          return encodeURIComponent(value);
        })
        .join("/")}`,
    match: (pathname: string) => matchRoute(segments, codecs, pathname),
  };
  return Object.freeze(compiled);
}

function splitTemplate(template: string): readonly string[] {
  if (!template.startsWith("/") || template === "/") {
    throw new Error("sink routes must be absolute and contain at least one segment");
  }
  if (template.includes("?") || template.includes("#") || template.includes("*")) {
    throw new Error("sink routes cannot contain queries, fragments, or wildcards");
  }
  const parts = template.slice(1).split("/");
  if (parts.some((part) => part.length === 0)) {
    throw new Error("sink routes cannot contain empty segments");
  }
  return parts;
}

function matchRoute<Codecs extends SinkParamCodecs>(
  segments: readonly Segment[],
  codecs: Codecs,
  pathname: string,
): SinkRouteMatch<DecodedSinkParams<Codecs>> {
  const actual = pathname.startsWith("/") ? pathname.slice(1).split("/") : [];
  if (actual.length !== segments.length) return { kind: "mismatch" };
  const params: Record<string, string> = {};
  for (const [index, segment] of segments.entries()) {
    const encoded = actual[index];
    if (encoded === undefined) return { kind: "mismatch" };
    if (segment.kind === "literal") {
      if (encoded !== segment.value) return { kind: "mismatch" };
      continue;
    }
    let decoded: string;
    try {
      decoded = decodeURIComponent(encoded);
    } catch (cause) {
      return { kind: "invalid", parameter: segment.name, detail: String(cause) };
    }
    if (decoded.includes("/") || decoded.includes("\\")) {
      return {
        kind: "invalid",
        parameter: segment.name,
        detail: "encoded path separators are not valid sink parameters",
      };
    }
    try {
      params[segment.name] = segment.codec.decode(decoded);
    } catch (cause) {
      return { kind: "invalid", parameter: segment.name, detail: String(cause) };
    }
  }
  if (!hasEveryDecodedParam(params, codecs)) {
    throw new TypeError("compiled sink route did not decode every declared parameter");
  }
  return { kind: "matched", params };
}

function hasEveryDecodedParam<Codecs extends SinkParamCodecs>(
  params: Readonly<Record<string, string>>,
  codecs: Codecs,
): params is DecodedSinkParams<Codecs> {
  return Object.keys(codecs).every((name) => name in params);
}
