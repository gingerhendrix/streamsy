/* oxlint-disable anti-slop/no-object-parameters -- Commit tokens are opaque object identities, not external records to parse. */
import { Context, Effect, Predicate } from "effect";
import type { Reader, Writer } from "../protocol/tags.ts";
import type { StreamsFault } from "../fault.ts";
import type { Storage } from "../storage/storage.ts";

/** Private bridge: a stream guard dies with this sentinel; the atomic runner translates it. */
const ForeignOwnerType = Symbol.for("@streamsy/core/internal/ForeignOwner");
export class ForeignOwner extends Error {
  readonly [ForeignOwnerType] = true;
  constructor(
    readonly stream: string,
    knownOwner: boolean,
  ) {
    super(
      knownOwner
        ? `Stream write to ${stream} does not share the checkpoint owner`
        : `Cannot establish a storage owner for ${stream}; the exposed Storage does not own the protocol wires`,
    );
  }
}
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This predicate narrows an unknown runtime defect at the package boundary.
export const isForeignOwner = (value: unknown): value is ForeignOwner =>
  Predicate.hasProperty(value, ForeignOwnerType) && value[ForeignOwnerType] === true;

type Owner = object;
const OwnerMetadata = Symbol.for("@streamsy/core/internal/CommitOwner");
export const attachOwner = <A extends object>(value: A, owner: Owner): A =>
  Object.defineProperty(value, OwnerMetadata, { value: owner, enumerable: false });
export const ownerOf = <A extends object>(value: A): Owner => {
  if (!(OwnerMetadata in value)) return value;
  // SAFETY: attachOwner is the only writer of the private owner metadata.
  return value[OwnerMetadata] as Owner;
};

export const CurrentOwner = Context.Reference<object | undefined>(
  "@streamsy/core/internal/CurrentOwner",
  { defaultValue: () => undefined },
);
export const checkWriteOwner = (owner: Owner, id: string, knownOwner: boolean) =>
  Effect.flatMap(CurrentOwner, (expected) =>
    expected === undefined || expected === owner
      ? Effect.void
      : Effect.die(new ForeignOwner(id, knownOwner)),
  );

// Metadata stays attached to the actual protocol wire, not an unrelated ambient Storage.
const Metadata = Symbol.for("@streamsy/core/internal/StorageOwner");
export const attachStorage = <A extends object>(wire: A, storage: typeof Storage.Service): A =>
  Object.defineProperty(wire, Metadata, { value: storage, enumerable: false });
export const storageOf = (
  wire: Reader<StreamsFault> | Writer<StreamsFault>,
): typeof Storage.Service | undefined => {
  if (!(Metadata in wire)) return undefined;
  // SAFETY: only attachStorage writes this private protocol metadata.
  return wire[Metadata] as typeof Storage.Service;
};
