/* oxlint-disable anti-slop/no-unsafe-dictionary-type, typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- Family construction is the checked bridge between route records and their mapped projection inputs; each assertion is documented at its use. */
import { Option, Schema } from "effect";
import { StreamRoute, StreamRef } from "@streamsy/core";
import type { InputMap, Slices } from "./batch.ts";
import { make, stream, type Fused, type Pinned } from "./projection.ts";
import * as Output from "./output.ts";
import { outputs, type Named } from "./outputs.ts";
import { bind, type Bound, type Fixed } from "./output-source.ts";
import type { OutputFold } from "./fold.ts";
import type { State } from "./state.ts";
import type { ProjectionFault } from "./fault.ts";
import type { Unit } from "./unit.ts";
import type { Effect } from "effect";

type AnyRoute = StreamRoute.StreamRoute<any, any, never, never>;
type RouteMap = Readonly<Record<string, AnyRoute>>;
type RouteParams<Route extends AnyRoute> =
  Route extends StreamRoute.StreamRoute<infer P, any, any, any> ? P : never;
type RouteItem<Route extends AnyRoute> =
  ReturnType<Route["ref"]> extends StreamRef.StreamRef<infer A> ? A : never;
type FamilyParams<Codecs extends StreamRoute.ParamCodecs> = StreamRoute.Params<Codecs>;

type InvalidRouteParam<Codecs extends StreamRoute.ParamCodecs, Route extends AnyRoute> = {
  readonly [Name in keyof RouteParams<Route>]: Name extends keyof FamilyParams<Codecs>
    ? RouteParams<Route>[Name] extends FamilyParams<Codecs>[Name]
      ? FamilyParams<Codecs>[Name] extends RouteParams<Route>[Name]
        ? never
        : Name
      : Name
    : Name;
}[keyof RouteParams<Route>];

type RouteRefs<Routes extends RouteMap> = {
  readonly [Name in keyof Routes]: ReturnType<Routes[Name]["ref"]>;
};

type RoutesWithin<Codecs extends StreamRoute.ParamCodecs, Routes extends RouteMap> = {
  readonly [Name in keyof Routes]: InvalidRouteParam<Codecs, Routes[Name]> extends never
    ? Routes[Name]
    : never;
};

type RouteWithin<Codecs extends StreamRoute.ParamCodecs, Route extends AnyRoute> =
  InvalidRouteParam<Codecs, Route> extends never ? Route : never;

interface FamilyCommon<Codecs extends StreamRoute.ParamCodecs, Routes extends RouteMap> {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params: Codecs;
  readonly inputs: Routes & RoutesWithin<Codecs, Routes>;
}

export interface FusedFamilyDefinition<
  Codecs extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  E,
  R,
> extends FamilyCommon<Codecs, Routes> {
  readonly process: (batch: Slices<RouteRefs<Routes>>, unit: Unit) => Effect.Effect<void, E, R>;
}

export interface PinnedFamilyDefinition<
  Codecs extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  OutRoute extends AnyRoute,
  E,
  R,
> extends FamilyCommon<Codecs, Routes> {
  readonly output: OutRoute & RouteWithin<Codecs, OutRoute>;
  readonly process: (
    batch: Slices<RouteRefs<Routes>>,
    unit: Unit,
  ) => Effect.Effect<ReadonlyArray<RouteItem<OutRoute>>, E, R>;
}

export type FamilyDefinition<
  Codecs extends StreamRoute.ParamCodecs = StreamRoute.ParamCodecs,
  Routes extends RouteMap = RouteMap,
  OutRoute extends AnyRoute = AnyRoute,
  E = unknown,
  R = unknown,
> =
  | FusedFamilyDefinition<Codecs, Routes, E, R>
  | PinnedFamilyDefinition<Codecs, Routes, OutRoute, E, R>;

export interface Family<
  Codecs extends StreamRoute.ParamCodecs = StreamRoute.ParamCodecs,
  Member =
    | Fused<InputMap, unknown, unknown>
    | Pinned<InputMap, unknown, unknown, unknown>
    | Named<InputMap, unknown, unknown>,
> {
  readonly _tag: "Family";
  readonly id: string;
  readonly params: Codecs;
  readonly member: (params: StreamRoute.Params<Codecs>) => Member;
  /** Tries input routes in declaration order. Shared inputs return only their declared subset. */
  readonly parse: (id: string) => Option.Option<Partial<StreamRoute.Params<Codecs>>>;
}

const encodeParams = <Codecs extends StreamRoute.ParamCodecs>(
  familyId: string,
  codecs: Codecs,
  params: StreamRoute.Params<Codecs>,
): Record<string, string> => {
  const values: Readonly<Record<string, unknown>> = params;
  return Object.fromEntries(
    Object.entries(codecs).map(([name, codec]) => {
      const encoded = Schema.encodeOption(codec)(values[name]);
      if (Option.isNone(encoded))
        throw new RangeError(`Cannot encode family ${familyId} parameter ${name}`);
      return [name, encoded.value];
    }),
  );
};

const refsOf = <Routes extends RouteMap>(
  routes: Routes,
  params: Readonly<Record<string, unknown>>,
): RouteRefs<Routes> => {
  const built: unknown = Object.fromEntries(
    Object.entries(routes).map(([name, route]) => [name, route.ref(params)]),
  );
  // The loop calls each route under its own key, so the resulting ref record
  // has exactly the mapped `RouteRefs<Routes>` shape.
  return built as RouteRefs<Routes>;
};

type InvalidOutputParam<C extends StreamRoute.ParamCodecs, P> = {
  readonly [K in keyof P]: K extends keyof FamilyParams<C>
    ? P[K] extends FamilyParams<C>[K]
      ? FamilyParams<C>[K] extends P[K]
        ? never
        : K
      : K
    : K;
}[keyof P];
type OutputsWithin<C extends StreamRoute.ParamCodecs, D extends Output.RoutedMap> = {
  readonly [K in keyof D]: D[K] extends { readonly stream: Output.Target<infer P> }
    ? InvalidOutputParam<C, P> extends never
      ? D[K]
      : never
    : D[K];
};
export interface NamedFamilyDefinition<
  C extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  D extends Output.RoutedMap,
  Result extends Output.Result<D>,
  E,
  R,
> extends FamilyCommon<C, Routes> {
  readonly outputs: D & OutputsWithin<C, D>;
  readonly process: (
    | ((batch: Slices<RouteRefs<Routes>>, unit: Unit) => Effect.Effect<Result, E, R>)
    | OutputFold<RouteRefs<Routes>, Output.StateOf<D>, Result, E, R>
  ) &
    (Exclude<keyof Result, keyof Output.Result<D>> extends never
      ? unknown
      : { readonly invalidOutputKeys: never });
}
export function family<
  C extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  const D extends Output.RoutedMap,
  Result extends Output.Result<D>,
  E,
  R,
>(
  definition: NamedFamilyDefinition<C, Routes, D, Result, E, R>,
): Family<
  C,
  Named<RouteRefs<Routes>, E | ProjectionFault, R | State> & {
    readonly outputs: Bound<Fixed<D>, {}>;
  }
> & { readonly outputs: Bound<D, FamilyParams<C>> };
export function family<Codecs extends StreamRoute.ParamCodecs, Routes extends RouteMap, E, R>(
  definition: FusedFamilyDefinition<Codecs, Routes, E, R>,
): Family<Codecs, Fused<RouteRefs<Routes>, E, R>>;
export function family<
  Codecs extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  OutRoute extends AnyRoute,
  E,
  R,
>(
  definition: PinnedFamilyDefinition<Codecs, Routes, OutRoute, E, R>,
): Family<Codecs, Pinned<RouteRefs<Routes>, RouteItem<OutRoute>, E, R>>;
export function family<
  Codecs extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  OutRoute extends AnyRoute,
  E,
  R,
>(
  definition:
    | FamilyDefinition<Codecs, Routes, OutRoute, E, R>
    | NamedFamilyDefinition<Codecs, Routes, Output.RoutedMap, any, E, R>,
) {
  const member = (params: StreamRoute.Params<Codecs>) => {
    if ("outputs" in definition) {
      // Each routed identity is resolved once; all other declaration fields are retained.
      const declarations = Object.fromEntries(
        Object.entries(definition.outputs).map(([name, output]) => {
          if (output._tag === "Value") return [name, output];
          const target = output.stream;
          // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Rows have string ids; routed targets expose ref.
          if (typeof target === "string" || !("ref" in target)) return [name, output];
          const ref = target.ref(params);
          return [
            name,
            {
              ...output,
              stream:
                output._tag === "Rows" ? ref.id : StreamRef.json(ref.id, { schema: output.schema }),
            },
          ];
        }),
      ) as Output.Map;
      return outputs({
        ...definition,
        params: encodeParams(definition.id, definition.params, params),
        inputs: refsOf(definition.inputs, params),
        outputs: declarations,
        process: definition.process,
      });
    }
    return "output" in definition
      ? stream({
          id: definition.id,
          version: definition.version,
          generation: definition.generation,
          params: encodeParams(definition.id, definition.params, params),
          inputs: refsOf(definition.inputs, params),
          output: definition.output.ref(params),
          process: definition.process,
        })
      : make({
          id: definition.id,
          version: definition.version,
          generation: definition.generation,
          params: encodeParams(definition.id, definition.params, params),
          inputs: refsOf(definition.inputs, params),
          process: definition.process,
        });
  };
  const parse = (id: string): Option.Option<Partial<StreamRoute.Params<Codecs>>> => {
    for (const route of Object.values(definition.inputs)) {
      const parsed = route.parse(id);
      if (Option.isSome(parsed)) return Option.some(parsed.value);
    }
    return Option.none();
  };
  const common = {
    _tag: "Family" as const,
    id: definition.id,
    params: definition.params,
    member,
    parse,
  };
  if (!("outputs" in definition)) return common;
  // The named branch above always returns a named member with the same output keys.
  const namedMember = member as (
    params: FamilyParams<Codecs>,
  ) => Named<RouteRefs<Routes>, E | ProjectionFault, R | State> & { readonly outputs: Output.Map };
  // Struct owns exactly the family codec keys and their decoded parameter types.
  const schema = Schema.Struct(definition.params) as StreamRoute.PathSchema<FamilyParams<Codecs>>;
  return { ...common, outputs: bind(definition.outputs, definition.id, schema, namedMember) };
}
