/* oxlint-disable typescript/no-explicit-any, anti-slop/no-unsafe-dictionary-type, typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- Declarations are the checked bridge between stream declarations and their mapped projection inputs; each assertion is documented at its use. */
import { Option, Schema } from "effect";
import { StreamAddress, type Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { RoutedMap, Target } from "./output-internal.ts";
import type * as Output from "./output.ts";

/** A stream declaration. `.at(params)` resolves it with the projection params. */
export type StreamDeclaration = StreamRoute.StreamRoute<any, any, never, never> &
  Streamsy.Any & {
    readonly at: (params: any) => Streamsy.Address<any, any>;
  };
/** A stream declaration, or a fixed address that `.at` does not rebind. */
export type Input = StreamDeclaration | Streamsy.Address<any, any>;
export type Inputs = Readonly<Record<string, Input>>;
export type AddressOf<X> = X extends StreamDeclaration
  ? ReturnType<X["at"]>
  : X extends Streamsy.Address<any, any>
    ? X
    : never;
export type Addresses<I extends Inputs> = { readonly [K in keyof I]: AddressOf<I[K]> };

type DeclaredParams<X> = X extends StreamRoute.StreamRoute<infer P, any, any, any> ? P : {};
type Invalid<C extends StreamRoute.ParamCodecs, P> = {
  readonly [Name in keyof P]: Name extends keyof StreamRoute.Params<C>
    ? P[Name] extends StreamRoute.Params<C>[Name]
      ? StreamRoute.Params<C>[Name] extends P[Name]
        ? never
        : Name
      : Name
    : Name;
}[keyof P];
type InvalidInput<C extends StreamRoute.ParamCodecs, X> = X extends StreamDeclaration
  ? Invalid<C, DeclaredParams<X>>
  : never;
/** Every declaration's params must be projection params with the same type. */
export type Within<C extends StreamRoute.ParamCodecs, I extends Inputs> = {
  readonly [K in keyof I]: InvalidInput<C, I[K]> extends never ? I[K] : never;
};
export type OneWithin<C extends StreamRoute.ParamCodecs, X> =
  InvalidInput<C, X> extends never ? X : never;
export type OutputsWithin<C extends StreamRoute.ParamCodecs, D extends RoutedMap> = {
  readonly [K in keyof D]: D[K] extends { readonly stream: Target<infer P> }
    ? Invalid<C, P> extends never
      ? D[K]
      : never
    : D[K];
};

/** Inert: `at` binds the params and returns the value runners take. */
export interface Declaration<Codecs extends StreamRoute.ParamCodecs, Bound> {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Codecs;
  /** Throws a RangeError when a param does not encode. */
  readonly at: (params: StreamRoute.Params<Codecs>) => Bound;
  /** Tries declaration inputs in order. Shared inputs return only their declared subset. */
  readonly parse: (id: string) => Option.Option<Partial<StreamRoute.Params<Codecs>>>;
}

export interface Common<C extends StreamRoute.ParamCodecs, I extends Inputs> {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params: C;
  readonly inputs: I & Within<C, I>;
}

/** Decoded params, keyed by name. */
export type DecodedParams = Readonly<Record<string, unknown>>;

export const isDeclaration = (input: Input): input is StreamDeclaration => "at" in input;

const encodeParams = <C extends StreamRoute.ParamCodecs>(
  id: string,
  codecs: C,
  params: StreamRoute.Params<C>,
): Record<string, string> => {
  const values: Readonly<Record<string, unknown>> = params;
  return Object.fromEntries(
    Object.entries(codecs).map(([name, codec]) => {
      const encoded = Schema.encodeOption(codec)(values[name]);
      if (Option.isNone(encoded))
        throw new RangeError(`Cannot encode projection ${id} parameter ${name}`);
      return [name, encoded.value];
    }),
  );
};

export const addressesOf = <I extends Inputs>(inputs: I, params: DecodedParams): Addresses<I> => {
  const built: unknown = Object.fromEntries(
    Object.entries(inputs).map(([name, input]) => [
      name,
      isDeclaration(input) ? input.at(params) : input,
    ]),
  );
  // Each entry is resolved under its own key, so the record has the mapped shape.
  return built as Addresses<I>;
};

/** Resolves each declared output target once. Fixed addresses and values are kept. */
export const resolveOutputs = (declarations: RoutedMap, params: DecodedParams): Output.Map =>
  Object.fromEntries(
    Object.entries(declarations).map(([name, output]) => {
      if (output._tag === "Value") return [name, output];
      const target = output.stream;
      if (!("address" in target)) return [name, output];
      const address = target.address(params);
      return [
        name,
        {
          ...output,
          stream: Object.assign(StreamAddress.json(address.id, { schema: output.schema }), {
            stream: target,
          }),
        },
      ];
    }),
  ) as Output.Map;

export const declare = <C extends StreamRoute.ParamCodecs, Bound>(
  definition: {
    readonly id: string;
    readonly version?: number;
    readonly generation?: number;
    readonly params: C;
    readonly inputs: Inputs;
  },
  bind: (key: {
    readonly id: string;
    readonly version: number;
    readonly generation: number;
    readonly params: Record<string, string>;
    readonly decoded: StreamRoute.Params<C>;
  }) => Bound,
): Declaration<C, Bound> => {
  const version = definition.version ?? 1;
  const generation = definition.generation ?? 1;
  return {
    id: definition.id,
    version,
    generation,
    params: definition.params,
    at: (params) =>
      bind({
        id: definition.id,
        version,
        generation,
        params: encodeParams(definition.id, definition.params, params),
        decoded: params,
      }),
    parse: (id) => {
      for (const input of Object.values<Input>(definition.inputs)) {
        if (!isDeclaration(input)) continue;
        const parsed = input.parse(id);
        if (Option.isSome(parsed)) return Option.some(parsed.value);
      }
      return Option.none();
    },
  };
};
