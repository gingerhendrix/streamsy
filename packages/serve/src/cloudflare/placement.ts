import { Option } from "effect";

export interface Placement {
  readonly name: (streamPath: string) => string;
}

/** The parse side of a stream, which is all a placement rule reads. */
interface Parser<Params> {
  parse(id: string): Option.Option<Params>;
}

/**
 * One rule in a stream placement: a stream and the object name its
 * addresses belong to.
 *
 * The owner takes the same parameters its own stream decodes, so the object name
 * and the storage routing agree by construction. `owner` uses method syntax on
 * purpose: it gives the parameter its contextually typed shape at the call site
 * and keeps the rule assignable to the erased table.
 */
interface OwnerRule<Params> {
  readonly stream: Parser<Params>;
  /** Names the object that owns every stream matching `stream`. */
  owner(params: Params): string;
}

/**
 * A rule after its parameter type is erased. `byRoute` reads each rule only
 * through that rule's own `parse`, so the erased parameter is never observed.
 */
interface ErasedOwnerRule {
  readonly stream: Parser<unknown>;
  owner(params: never): string;
}

/**
 * Declare one placement rule with its stream's parameters in scope.
 *
 * The helper exists so the owner parameter is typed and checked. A rule passed
 * to `Placement.byRoute` without it would see the erased parameter set instead.
 */
export const rule = <Params>(spec: OwnerRule<Params>): ErasedOwnerRule => spec;

export const Placement = {
  byStream: (): Placement => ({ name: (streamPath) => streamPath }),
  byKey: (key: (streamPath: string) => string): Placement => ({ name: key }),
  /**
   * The first matching rule names the object.
   *
   * A path that no rule matches yields `""`, which the router turns into
   * `400 Invalid placement key`. An owner that throws is a `500`. A rule whose
   * stream cannot decode a segment does not own that path, so the table falls
   * through to the next rule.
   */
  byRoute: (rules: ReadonlyArray<ErasedOwnerRule>): Placement => ({
    name: (streamPath) => {
      for (const entry of rules) {
        const params = entry.stream.parse(streamPath);

        if (Option.isNone(params)) continue;

        // SAFETY: `entry.stream.parse` decoded this value through the same
        // stream whose owner reads it, so the parameter set is the one the
        // owner was declared with.
        // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- The invariant is stated in the comment above.
        return entry.owner(params.value as never);
      }

      return "";
    },
  }),
} as const;
