/**
 * The domains this application partitions by, and the keys that name them.
 *
 * B3 keyed the host by a workspace id, because a workspace was the only thing
 * a partition could be. That was never the general rule — it was the one case
 * the tracker had. A *domain* is the general rule: a kind of thing that owns
 * durable state, and a `PartitionKey` is one instance of such a thing.
 *
 * Three domains exist and each is load-bearing:
 *
 * - **workspace** — the issue application. One workspace's facts, rows,
 *   receipts, streams and outbox. Unchanged from B3, including its on-disk
 *   layout and every route that reaches it.
 * - **user** — a person's cross-workspace inbox. Keyed by a user id, so a
 *   user's rows live in a partition no workspace runtime can reach.
 * - **global** — exactly one partition, holding what belongs to the host
 *   rather than to any workspace or user: the exchange cursors.
 *
 * A key is a *value*, not a string, so a workspace id and a user id can never
 * be confused for one another by a host that is handed one of them. The id
 * inside a key is checked against the domain's own identifier pattern, which
 * admits no separator and no traversal — which is also what makes it usable
 * directly as a directory name.
 */
import { Schema } from "effect";
import { Identifier, IDENTIFIER_PATTERN } from "./issue.ts";

export const DOMAIN_KINDS = ["workspace", "user", "global"] as const;

export const DomainKind = Schema.Literals(DOMAIN_KINDS);
export type DomainKind = typeof DomainKind.Type;

/**
 * The one id the global domain has.
 *
 * The global domain is a singleton by definition, so its key still carries an
 * id — a key with a hole in it would make every consumer branch on the kind
 * before it could use the value.
 */
export const GLOBAL_PARTITION_ID = "global";

export const PartitionKey = Schema.Struct({ kind: DomainKind, id: Identifier });

/**
 * A key, as a discriminated union rather than a struct with a `kind` field.
 *
 * The union is what lets a caller that names a domain be handed that domain's
 * partition: `partition(userKey("ada"))` is typed as a user partition, and the
 * host needs no runtime narrowing to say so.
 */
export interface WorkspaceKey {
  readonly kind: "workspace";
  readonly id: string;
}
export interface UserKey {
  readonly kind: "user";
  readonly id: string;
}
export interface GlobalKey {
  readonly kind: "global";
  readonly id: string;
}
export type PartitionKey = WorkspaceKey | UserKey | GlobalKey;

export const decodePartitionKey = Schema.decodeUnknownSync(PartitionKey);

export const workspaceKey = (id: string): WorkspaceKey => ({ kind: "workspace", id });
export const userKey = (id: string): UserKey => ({ kind: "user", id });
export const globalKey = (): GlobalKey => ({ kind: "global", id: GLOBAL_PARTITION_ID });

/**
 * A key as one flat string, for map lookup, metrics and log lines.
 *
 * The separator is a colon, which the identifier pattern excludes, so the
 * encoding is unambiguous and `parsePartitionKey` is its exact inverse.
 */
export function partitionKeyString(key: PartitionKey): string {
  return `${key.kind}:${key.id}`;
}

export function partitionKeyEquals(left: PartitionKey, right: PartitionKey): boolean {
  return left.kind === right.kind && left.id === right.id;
}

export function parsePartitionKey(value: string): PartitionKey | undefined {
  const separator = value.indexOf(":");
  if (separator <= 0) return undefined;
  const kind = asDomainKind(value.slice(0, separator));
  const id = value.slice(separator + 1);
  if (kind === undefined || !isDomainId(kind, id)) return undefined;
  if (kind === "workspace") return workspaceKey(id);
  return kind === "user" ? userKey(id) : globalKey();
}

/** The domain this name refers to, if this host has one. */
export function asDomainKind(value: string): DomainKind | undefined {
  return DOMAIN_KINDS.find((kind) => kind === value);
}

/**
 * Whether an id is one this domain will accept as a partition key.
 *
 * The global domain accepts exactly one id, so a request that names any other
 * global partition is refused rather than quietly served by the only one.
 */
export function isDomainId(kind: DomainKind, id: string): boolean {
  if (!IDENTIFIER_PATTERN.test(id)) return false;
  return kind === "global" ? id === GLOBAL_PARTITION_ID : true;
}

/**
 * The directory segment one domain's partitions live under.
 *
 * `workspace` is `workspaces`, which is what B3 wrote, so a pre-B4 data
 * directory is read back unchanged by a B4 host.
 */
export const DOMAIN_DIRECTORIES = {
  workspace: "workspaces",
  user: "users",
  global: "global",
} satisfies Readonly<Record<DomainKind, string>>;

/** The path segments naming one partition's durable resources, relative to a root. */
export function partitionSegments(key: PartitionKey): readonly string[] {
  return [DOMAIN_DIRECTORIES[key.kind], key.id];
}
