import { Context, Record } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Input, StreamDeclaration } from "./declaration.ts";
import type { ResolvedOutput } from "./commit/internal.ts";

const TypeId = Symbol.for("@streamsy/projection/PerGeneration");

export interface PerGeneration<D extends StreamDeclaration, N extends string> {
  readonly [TypeId]: { readonly route: D; readonly param: N; readonly shared: boolean };
}

export type RoutedOutput = Input | PerGeneration<StreamDeclaration, string>;

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
export const perGeneration = <D extends StreamDeclaration, const N extends NumberParams<D>>(
  route: D,
  param: N,
  options?: { readonly shared?: boolean },
): PerGeneration<D, N> => ({ [TypeId]: { route, param, shared: options?.shared ?? false } });

export const isPerGeneration = (
  value: RoutedOutput,
): value is PerGeneration<StreamDeclaration, string> => TypeId in value;

export const generationBinding = <D extends StreamDeclaration, N extends string>(
  binding: PerGeneration<D, N>,
) => binding[TypeId];

export const validateGenerationParam = (
  output: RoutedOutput,
  params: StreamRoute.ParamCodecs,
): void => {
  if (!isPerGeneration(output)) return;
  const { param } = generationBinding(output);

  if (Record.has(params, param))
    throw new RangeError(`Generation-bound parameter ${param} must not be a projection parameter`);
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
