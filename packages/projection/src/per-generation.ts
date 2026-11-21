import { Array, Context, Record, Result } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Input, StreamDeclaration } from "./declaration.ts";
import type { ResolvedOutput } from "./commit/internal.ts";

const TypeId = Symbol.for("@streamsy/projection/PerGeneration");

export interface PerGeneration<
  D extends StreamDeclaration,
  N extends string,
  By extends string = never,
> {
  readonly [TypeId]: {
    readonly route: D;
    readonly param: N;
    readonly shared: boolean;
    readonly routeBy?: readonly By[];
  };
}

export type RoutedOutput = Input | PerGeneration<StreamDeclaration, string, string>;

export type DeclaredParams<D> =
  D extends StreamRoute.StreamRoute<infer P, infer _A, infer _RD, infer _RE> ? P : {};

type NumberParams<D, P = DeclaredParams<D>> = {
  readonly [N in keyof P]-?: unknown extends P[N]
    ? never
    : [P[N], number] extends [number, P[N]]
      ? N
      : never;
}[keyof P] &
  string;

/** Bind one numeric output route parameter to the projection's generation. */
export function perGeneration<
  D extends StreamDeclaration,
  const N extends NumberParams<D>,
  const By extends Exclude<keyof DeclaredParams<D> & string, N>,
>(
  route: D,
  param: N,
  options: { readonly routeBy: readonly [By, ...By[]]; readonly shared?: false },
): PerGeneration<D, N, By>;
export function perGeneration<D extends StreamDeclaration, const N extends NumberParams<D>>(
  route: D,
  param: N,
  options?: { readonly shared?: boolean; readonly routeBy?: never },
): PerGeneration<D, N>;
export function perGeneration<D extends StreamDeclaration, N extends string>(
  route: D,
  param: N,
  options?: { readonly shared?: boolean; readonly routeBy?: readonly string[] },
): PerGeneration<D, N, string> {
  const shared = options?.shared ?? false;
  const routeBy = options?.routeBy;

  return {
    [TypeId]:
      routeBy === undefined
        ? { route, param, shared }
        : { route, param, shared, routeBy: Object.freeze([...routeBy]) },
  };
}

export const isPerGeneration = (
  value: RoutedOutput,
): value is PerGeneration<StreamDeclaration, string, string> => TypeId in value;

export const generationBinding = <
  D extends StreamDeclaration,
  N extends string,
  By extends string = never,
>(
  binding: PerGeneration<D, N, By>,
) => binding[TypeId];

export const validateGenerationParam = (
  output: RoutedOutput,
  params: StreamRoute.ParamCodecs,
): void => {
  if (!isPerGeneration(output)) return;
  const { route, param, routeBy, shared } = generationBinding(output);

  if (Record.has(params, param))
    throw new RangeError(`Generation-bound parameter ${param} must not be a projection parameter`);

  if (routeBy === undefined) return;

  if (shared) throw new RangeError("Routed outputs cannot be shared");

  if (routeBy.length === 0 || new Set(routeBy).size !== routeBy.length)
    throw new RangeError("routeBy requires one or more distinct route parameters");

  const names = Array.filterMap(route.template.split("/"), (part) =>
    part.startsWith(":") ? Result.succeed(part.slice(1)) : Result.fail(undefined),
  );

  for (const name of routeBy) {
    if (!names.includes(name))
      throw new RangeError(`Routed parameter ${name} must be a route parameter`);

    if (name === param || Record.has(params, name))
      throw new RangeError(
        `Routed parameter ${name} must not be a generation or projection parameter`,
      );
  }

  for (const name of names) {
    if (name !== param && !routeBy.includes(name) && !Record.has(params, name))
      throw new RangeError(`Missing projection parameter ${name} for routed output`);
  }
};

/** Private metadata on the bound value. */
export const GenerationOutputs = Symbol.for("@streamsy/projection/internal/GenerationOutputs");

export interface GenerationOutputBindings {
  readonly [GenerationOutputs]?: ReadonlyMap<string, { readonly shared: boolean }>;
}

/** Outputs whose creation is managed by the current transaction. */
export const TransactionGenerationOutputs = Context.Reference<
  ReadonlyMap<string, ResolvedOutput> | undefined
>("@streamsy/projection/internal/TransactionGenerationOutputs", { defaultValue: () => undefined });
