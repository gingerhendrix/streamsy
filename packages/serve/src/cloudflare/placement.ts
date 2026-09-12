import { Option } from "effect";

export interface Placement {
  readonly name: (streamPath: string) => string;
}

/** The parse side of a stream family, which is all a placement rule reads. */
export interface FamilyRoute<Params> {
  parse(id: string): Option.Option<Params>;
}

/**
 * One rule in a route placement: a stream family and the object name its
 * members belong to.
 *
 * The owner takes the same parameters its own route decodes, so the object name
 * and the storage routing agree by construction. `owner` uses method syntax on
 * purpose: it gives the parameter its contextually typed shape at the call site
 * and keeps the rule assignable to the erased table.
 */
export interface OwnerRule<Params> {
  readonly route: FamilyRoute<Params>;
  /** Names the object that owns every stream matching `route`. */
  owner(params: Params): string;
}

/**
 * A rule after its parameter type is erased. `byRoute` reads each rule only
 * through that rule's own `parse`, so the erased parameter is never observed.
 */
export interface ErasedOwnerRule {
  readonly route: FamilyRoute<unknown>;
  owner(params: never): string;
}

/**
 * Declare one placement rule with its family's parameters in scope.
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
   * route cannot decode a segment does not own that path, so the table falls
   * through to the next rule.
   */
  byRoute: (rules: ReadonlyArray<ErasedOwnerRule>): Placement => ({
    name: (streamPath) => {
      for (const entry of rules) {
        const params = entry.route.parse(streamPath);
        if (Option.isNone(params)) continue;
        // SAFETY: `entry.route.parse` decoded this value through the same
        // family whose owner reads it, so the parameter set is the one the
        // owner was declared with.
        // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- The invariant is stated in the comment above.
        return entry.owner(params.value as never);
      }
      return "";
    },
  }),
} as const;
