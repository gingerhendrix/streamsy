import type { Streamsy } from "@streamsy/core";
import type { Stream, Rows, Declaration } from "./output.ts";

/** Output declarations own their codec and retain the target stream key. */
export type Target<P> = Streamsy.Any & {
  readonly address: (params: P) => { readonly id: string };
};
export type TargetServices<T> = T extends { readonly stream: { readonly Identifier: infer R } }
  ? R
  : T extends { readonly Identifier: infer R }
    ? R
    : never;
export type RoutedMap = Readonly<
  Record<string, Stream<any, Target<any>> | Rows<any, Target<any>> | Declaration>
>;
