/* oxlint-disable anti-slop/no-unsafe-dictionary-type, typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- Family construction is the checked bridge between route records and their mapped projection inputs; each assertion is documented at its use. */
import { Option, Schema } from "effect";
import { StreamRoute, type StreamRef } from "@streamsy/core";
import type { InputMap, Slices } from "./batch.ts";
import { make, stream, type Fused, type Pinned } from "./projection.ts";
import type { Unit } from "./unit.ts";
import type { Effect } from "effect";

type AnyRoute = StreamRoute.StreamRoute<any, any, never, never>;
type RouteMap = Readonly<Record<string, AnyRoute>>;
type RouteParams<Route extends AnyRoute> =
  Route extends StreamRoute.StreamRoute<infer P, any, any, any> ? P : never;
type RouteItem<Route extends AnyRoute> =
  ReturnType<Route["ref"]> extends StreamRef.StreamRef<infer A> ? A : never;

type RouteRefs<Routes extends RouteMap> = {
  readonly [Name in keyof Routes]: ReturnType<Routes[Name]["ref"]>;
};

type RoutesWithin<Codecs extends StreamRoute.ParamCodecs, Routes extends RouteMap> = {
  readonly [Name in keyof Routes]: Exclude<
    keyof RouteParams<Routes[Name]>,
    keyof Codecs
  > extends never
    ? Routes[Name]
    : never;
};

type RouteWithin<Codecs extends StreamRoute.ParamCodecs, Route extends AnyRoute> =
  Exclude<keyof RouteParams<Route>, keyof Codecs> extends never ? Route : never;

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
  Output extends AnyRoute,
  E,
  R,
> extends FamilyCommon<Codecs, Routes> {
  readonly output: Output & RouteWithin<Codecs, Output>;
  readonly process: (
    batch: Slices<RouteRefs<Routes>>,
    unit: Unit,
  ) => Effect.Effect<ReadonlyArray<RouteItem<Output>>, E, R>;
}

export type FamilyDefinition<
  Codecs extends StreamRoute.ParamCodecs = StreamRoute.ParamCodecs,
  Routes extends RouteMap = RouteMap,
  Output extends AnyRoute = AnyRoute,
  E = unknown,
  R = unknown,
> =
  | FusedFamilyDefinition<Codecs, Routes, E, R>
  | PinnedFamilyDefinition<Codecs, Routes, Output, E, R>;

export interface Family<
  Codecs extends StreamRoute.ParamCodecs = StreamRoute.ParamCodecs,
  Member = Fused<InputMap, unknown, unknown> | Pinned<InputMap, unknown, unknown, unknown>,
> {
  readonly id: string;
  readonly params: Codecs;
  readonly member: (params: StreamRoute.Params<Codecs>) => Member;
  /** Tries input routes in declaration order. Shared inputs return only their declared subset. */
  readonly parse: (id: string) => Option.Option<Partial<StreamRoute.Params<Codecs>>>;
}

const encodeParams = <Codecs extends StreamRoute.ParamCodecs>(
  codecs: Codecs,
  params: StreamRoute.Params<Codecs>,
): Record<string, string> => {
  const values: Readonly<Record<string, unknown>> = params;
  return Object.fromEntries(
    Object.entries(codecs).map(([name, codec]) => [name, Schema.encodeSync(codec)(values[name])]),
  );
};

const refsOf = <Routes extends RouteMap>(
  routes: Routes,
  params: Readonly<Record<string, unknown>>,
): RouteRefs<Routes> =>
  Object.fromEntries(
    Object.entries(routes).map(([name, route]) => [name, route.ref(params)]),
  ) as unknown as RouteRefs<Routes>;

export function family<Codecs extends StreamRoute.ParamCodecs, Routes extends RouteMap, E, R>(
  definition: FusedFamilyDefinition<Codecs, Routes, E, R>,
): Family<Codecs, Fused<RouteRefs<Routes>, E, R>>;
export function family<
  Codecs extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  Output extends AnyRoute,
  E,
  R,
>(
  definition: PinnedFamilyDefinition<Codecs, Routes, Output, E, R>,
): Family<Codecs, Pinned<RouteRefs<Routes>, RouteItem<Output>, E, R>>;
export function family<
  Codecs extends StreamRoute.ParamCodecs,
  Routes extends RouteMap,
  Output extends AnyRoute,
  E,
  R,
>(definition: FamilyDefinition<Codecs, Routes, Output, E, R>) {
  const member = (params: StreamRoute.Params<Codecs>) => {
    return "output" in definition
      ? stream({
          id: definition.id,
          version: definition.version,
          generation: definition.generation,
          params: encodeParams(definition.params, params),
          inputs: refsOf(definition.inputs, params),
          output: definition.output.ref(params),
          process: definition.process,
        })
      : make({
          id: definition.id,
          version: definition.version,
          generation: definition.generation,
          params: encodeParams(definition.params, params),
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
  return { id: definition.id, params: definition.params, member, parse };
}
