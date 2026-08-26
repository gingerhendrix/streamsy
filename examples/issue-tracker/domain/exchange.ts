/**
 * Cross-domain exchange: how one domain's facts become another domain's rows.
 *
 * An exchange is declared, not written. What it declares is *placement*: which
 * domain each side lives in, and which field of a record names the partition
 * inside that domain. Placement is what makes the exchange checkable — a
 * declaration whose two sides are the same placement is not an exchange at all
 * but a view, and it is refused where it is written rather than where it runs.
 *
 * Three checks exist and they are deliberately at three different times:
 *
 * 1. **Declaration time.** The two placements must differ, a keyed domain must
 *    name a key field, and the singleton global domain must not. A failure
 *    here is a `InvalidExchangeDeclaration` thrown as the module loads, so a
 *    host carrying a malformed exchange cannot start.
 * 2. **Before a record moves.** Both the source key and the destination key
 *    are extracted and checked against their domain's identifier rule. A
 *    failure here is a typed `ExchangeKeyMismatch` value — the pass reports it
 *    and advances no cursor, so nothing is written half-keyed.
 * 3. **After projection.** The projected row's own key field must equal the
 *    destination partition key the record was routed by. This is the check
 *    that would catch a projection quietly writing user A's fact into user B's
 *    partition, and it is why the row key is re-read from the row rather than
 *    assumed from the routing decision.
 *
 * The cursor is declared here too, and it is its own domain on purpose. See
 * {@link ExchangeCursor}.
 */
import { Schema } from "effect";
import {
  DomainKind,
  globalKey,
  isDomainId,
  PartitionKey,
  userKey,
  workspaceKey,
} from "./domains.ts";
import { Identifier, IssueStatus, Sequence, Timestamp } from "./issue.ts";
import { inboxIdOf, type InboxRow } from "./inbox.ts";

/**
 * The exchange's resume position.
 *
 * This is a *separate position domain* and the type says so. `domain` is a
 * literal that no other position in this repository carries, so a value that
 * is not an exchange cursor cannot decode as one: not an A4
 * `HistoryPosition`, which is a store checkpoint over a plan identity, and not
 * a native Durable Streams offset, which is an opaque token a client resumes a
 * *stream* with. Mixing those domains is the failure gate 8 exists to prevent,
 * and the exchange is exactly the kind of component that would be tempted to.
 *
 * `arrival` is what the position actually is: how many source records this
 * exchange has consumed, in the order the source's durable log presented them.
 * It is a count in the application's own domain, so the exchange never holds a
 * native offset at all — there is nothing here to mix up.
 */
export const EXCHANGE_CURSOR_DOMAIN = "issue-tracker.exchange-cursor/1";

export const ExchangeCursor = Schema.Struct({
  /** The position domain. Present so a foreign position cannot decode as one. */
  domain: Schema.Literal(EXCHANGE_CURSOR_DOMAIN),
  exchange: Schema.String,
  version: Sequence,
  source: PartitionKey,
  /** Source records consumed, in arrival order. Not an offset, and not a checkpoint. */
  arrival: Sequence,
  /**
   * Rows this exchange has written into destination inboxes.
   *
   * At-least-once, and reported rather than resumed from. Delivery is made safe
   * by `inboxId` idempotence rather than by this counter, so a page replayed
   * after a crash between the inbox write and this cursor write is counted
   * twice: the number can exceed the distinct rows that exist. Read it as work
   * done, never as a row count.
   */
  applied: Sequence,
});
export type ExchangeCursor = typeof ExchangeCursor.Type;

export const decodeExchangeCursor = Schema.decodeUnknownSync(ExchangeCursor);
export const decodeExchangeCursorEffect = Schema.decodeUnknownEffect(ExchangeCursor);

/** The position an exchange starts from when it has never run against a source. */
export function initialCursor(
  exchange: string,
  version: number,
  source: PartitionKey,
): ExchangeCursor {
  return {
    domain: EXCHANGE_CURSOR_DOMAIN,
    exchange,
    version,
    source,
    arrival: 0,
    applied: 0,
  };
}

/** A declaration that could not be built. Thrown where it is written. */
export class InvalidExchangeDeclaration extends Schema.TaggedError<InvalidExchangeDeclaration>()(
  "InvalidExchangeDeclaration",
  { exchange: Schema.String, detail: Schema.String },
) {}

/** A record whose key does not fit the domain it was declared to be placed in. */
export class ExchangeKeyMismatch extends Schema.TaggedError<ExchangeKeyMismatch>()(
  "ExchangeKeyMismatch",
  {
    exchange: Schema.String,
    side: Schema.Literals(["source", "destination", "row"]),
    domain: DomainKind,
    keyField: Schema.String,
    detail: Schema.String,
  },
) {}

/**
 * A key as it is read off a record, before any domain has accepted it.
 *
 * `undefined` is a real answer, not an error case swallowed: a record that does
 * not carry the field its placement names has no key, and saying so is what
 * turns a missing key into a typed mismatch rather than the string
 * `"undefined"` quietly becoming a partition id.
 */
export type UncheckedKey = string | undefined;

/**
 * Where one side of an exchange lives.
 *
 * `keyField` is the field *name*, and `key` reads it. Both are declared: the
 * name is what a failure reports and what a reviewer checks against the
 * schema, and the reader is what actually runs.
 */
export interface DomainPlacement<T> {
  readonly domain: DomainKind;
  /** `null` only for the singleton global domain, which has no key to read. */
  readonly keyField: string | null;
  readonly key: (value: T) => UncheckedKey;
}

export interface ExchangeDeclaration<Record, Row> {
  readonly name: string;
  readonly version: number;
  readonly source: DomainPlacement<Record>;
  readonly destination: DomainPlacement<Record>;
  /** How the destination row names the user it belongs to. Checked after projection. */
  readonly rowKeyField: string;
  readonly rowKey: (row: Row) => UncheckedKey;
  readonly project: (record: Record) => Row;
}

export interface ExchangeDefinition<Record, Row> extends ExchangeDeclaration<Record, Row> {
  /** The source partition this record belongs to, or the typed mismatch. */
  readonly sourceKey: (record: Record) => PartitionKey | ExchangeKeyMismatch;
  /** The destination partition this record belongs to, or the typed mismatch. */
  readonly destinationKey: (record: Record) => PartitionKey | ExchangeKeyMismatch;
  /** Project one record and check the row lands where the record was routed. */
  readonly rowFor: (record: Record, destination: PartitionKey) => Row | ExchangeKeyMismatch;
}

/**
 * Build one exchange, checking its placement as it is built.
 *
 * @throws InvalidExchangeDeclaration when the two placements cannot describe a
 * cross-domain edge.
 */
export function defineExchange<Record, Row>(
  declaration: ExchangeDeclaration<Record, Row>,
): ExchangeDefinition<Record, Row> {
  const { name, source, destination } = declaration;
  checkPlacement(name, "source", source);
  checkPlacement(name, "destination", destination);
  if (source.domain === destination.domain && source.keyField === destination.keyField) {
    throw new InvalidExchangeDeclaration({
      exchange: name,
      detail: `both sides are placed in ${source.domain} by ${String(source.keyField)}: that is a view, not an exchange`,
    });
  }
  if (declaration.rowKeyField.length === 0) {
    throw new InvalidExchangeDeclaration({
      exchange: name,
      detail: "the destination row must name the key it is placed by",
    });
  }

  const keyFor = (
    side: "source" | "destination",
    placement: DomainPlacement<Record>,
    record: Record,
  ): PartitionKey | ExchangeKeyMismatch => {
    const keyField = placement.keyField ?? "";
    if (placement.domain === "global") return globalKey();
    const raw = placement.key(record);
    if (raw === undefined) {
      return new ExchangeKeyMismatch({
        exchange: name,
        side,
        domain: placement.domain,
        keyField,
        detail: `the record carries no ${keyField}`,
      });
    }
    if (!isDomainId(placement.domain, raw)) {
      return new ExchangeKeyMismatch({
        exchange: name,
        side,
        domain: placement.domain,
        keyField,
        detail: `${raw.slice(0, 80)} is not a ${placement.domain} identifier`,
      });
    }
    return placement.domain === "workspace" ? workspaceKey(raw) : userKey(raw);
  };

  return {
    ...declaration,
    sourceKey: (record) => keyFor("source", source, record),
    destinationKey: (record) => keyFor("destination", destination, record),
    rowFor: (record, target) => {
      const row = declaration.project(record);
      const placed = declaration.rowKey(row);
      if (placed !== target.id) {
        return new ExchangeKeyMismatch({
          exchange: name,
          side: "row",
          domain: destination.domain,
          keyField: declaration.rowKeyField,
          detail: `projected row is placed at ${placed ?? "no key"}, routed to ${target.id}`,
        });
      }
      return row;
    },
  };
}

function checkPlacement<Record>(
  name: string,
  side: "source" | "destination",
  placement: DomainPlacement<Record>,
): void {
  if (placement.domain === "global") {
    if (placement.keyField !== null) {
      throw new InvalidExchangeDeclaration({
        exchange: name,
        detail: `${side} is global, which is a singleton and has no key field`,
      });
    }
    return;
  }
  if (placement.keyField === null || placement.keyField.length === 0) {
    throw new InvalidExchangeDeclaration({
      exchange: name,
      detail: `${side} is placed in ${placement.domain} but names no key field`,
    });
  }
}

/**
 * One assignment, as the exchange reads it out of a workspace's durable log.
 *
 * It is the canonical `IssueAssigned` fact plus the arrival index the exchange
 * observed it at. Nothing is enriched from maintained state, so the record is
 * a pure function of the durable fact and its position.
 */
export const AssignmentActivity = Schema.Struct({
  workspaceId: Identifier,
  issueId: Identifier,
  assigneeId: Identifier,
  status: IssueStatus,
  eventId: Identifier,
  occurredAt: Timestamp,
  sequence: Sequence,
  arrival: Sequence,
});
export type AssignmentActivity = typeof AssignmentActivity.Type;

export const decodeAssignmentActivity = Schema.decodeUnknownSync(AssignmentActivity);

/**
 * The tracker's one exchange: workspace assignment activity into a user inbox.
 *
 * The source is placed by `workspaceId` in the workspace domain; the
 * destination is placed by `assigneeId` in the user domain. Those are two
 * different fields of the same record, which is the whole point — the edge
 * crosses domains *because* the record names two different partitions.
 */
export const assignmentInbox: ExchangeDefinition<AssignmentActivity, InboxRow> = defineExchange({
  name: "issue-tracker.assignment-inbox",
  version: 1,
  source: { domain: "workspace", keyField: "workspaceId", key: (r) => r.workspaceId },
  destination: { domain: "user", keyField: "assigneeId", key: (r) => r.assigneeId },
  rowKeyField: "userId",
  rowKey: (row) => row.userId,
  project: (record) => ({
    inboxId: inboxIdOf(record.workspaceId, record.eventId),
    userId: record.assigneeId,
    workspaceId: record.workspaceId,
    issueId: record.issueId,
    status: record.status,
    eventId: record.eventId,
    occurredAt: record.occurredAt,
    sequence: record.sequence,
    arrival: record.arrival,
  }),
});
