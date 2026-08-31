/**
 * Typed failures the user and global domains can produce.
 *
 * They are kept apart from `errors.ts` for the same reason `host-errors.ts` is:
 * `errors.ts` describes what can go wrong while serving *one workspace's*
 * issues, and its union is what the workspace router exhaustively translates.
 * An inbox that cannot be read, or an exchange cursor that no longer decodes,
 * belongs to a different partition with a different router, and folding them
 * into one union would make each router carry branches it can never reach.
 */
import { Schema } from "effect";

/** The user partition's inbox store itself failed. Never a validation outcome. */
export class InboxUnavailable extends Schema.TaggedError<InboxUnavailable>()("InboxUnavailable", {
  operation: Schema.String,
  detail: Schema.String,
}) {}

/** A durable inbox row that the declared schema no longer accepts. Fail-stop. */
export class InboxRestorePoison extends Schema.TaggedError<InboxRestorePoison>()(
  "InboxRestorePoison",
  { userId: Schema.String, key: Schema.String, detail: Schema.String },
) {}

/** The global partition's exchange cursor store itself failed. */
export class ExchangeStoreUnavailable extends Schema.TaggedError<ExchangeStoreUnavailable>()(
  "ExchangeStoreUnavailable",
  { operation: Schema.String, detail: Schema.String },
) {}

/**
 * A durable cursor that no longer decodes as an exchange cursor.
 *
 * This is the failure that catches a foreign position being written into the
 * cursor's storage — a native offset, or an A4 checkpoint. It is fail-stop:
 * resuming from a position whose domain is unknown is exactly the mixing gate
 * 8 forbids, and starting over would silently re-exchange everything.
 */
export class ExchangeCursorPoison extends Schema.TaggedError<ExchangeCursorPoison>()(
  "ExchangeCursorPoison",
  { exchange: Schema.String, source: Schema.String, detail: Schema.String },
) {}

export type DomainError =
  | InboxUnavailable
  | InboxRestorePoison
  | ExchangeStoreUnavailable
  | ExchangeCursorPoison;
