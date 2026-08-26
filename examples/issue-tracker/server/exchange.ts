/**
 * The resumable cross-domain exchange, as a host-level component.
 *
 * The exchange is not a service inside anybody's layer, and that is the whole
 * design. A service inside a workspace partition that could reach a user
 * partition would put back exactly the coupling B3's keying removed: the
 * workspace runtime would once again be able to touch state it does not own.
 * Instead the exchange sits *above* the partitions, holds its own cursor, and
 * drives each side through that side's own runtime — so every read runs where
 * the source's services live, every write runs where the destination's
 * services live, and neither runtime ever sees the other.
 *
 * One pass over one source is:
 *
 * 1. lease the global partition and read the cursor;
 * 2. lease the source partition and read the records after that position;
 * 3. check every record's placement — source key, destination key, and the
 *    projected row's own key — *before* anything is written;
 * 4. lease each destination partition and apply its rows, idempotently;
 * 5. advance the cursor.
 *
 * Leases are what make steps 2 to 5 safe: a leased partition is not evictable
 * and not sweepable, so the idle policy cannot close a runtime that a pass is
 * halfway through using. They are released in a `finally`, including on the
 * failure paths, so a failed pass never pins a partition open.
 *
 * Steps 4 and 5 are two durable writes in two different partitions, so a host
 * that dies between them replays step 4 on its next pass. That is safe rather
 * than merely tolerated: an inbox row's key is derived from the source
 * workspace and the canonical event id, so re-applying a record writes the
 * same row again and the inbox is unchanged. Delivery is at-least-once; the
 * *result* is exactly-once.
 *
 * A pass that fails — a mis-keyed record, an unreadable source, a destination
 * that will not open — advances no cursor at all. Fail-stop is the same law
 * the source and State ingestion paths already follow: a position that moved
 * past work that was not done is a silent hole.
 */
import { Effect } from "effect";
import {
  assignmentInbox,
  ExchangeKeyMismatch,
  type AssignmentActivity,
  type ExchangeCursor,
} from "../domain/exchange.ts";
import type { InboxRow } from "../domain/inbox.ts";
import { partitionKeyEquals, partitionKeyString, type PartitionKey } from "../domain/domains.ts";
import type { ApplicationServices } from "./application.ts";
import { readAssignmentActivity, EXCHANGE_SOURCE_PAGE_LIMIT } from "./exchange-source.ts";
import { ExchangeCursorStore } from "./exchange-store.ts";
import { ExchangeSourceRegistry } from "./source-registry.ts";
import type { StreamGateway } from "./gateway.ts";
import type { GlobalServices } from "./global-domain.ts";
import { hostFailureReport, type HostFailure } from "./host-errors.ts";
import { applyInbox, type UserServices } from "./user-domain.ts";

/**
 * A partition held open for the duration of a pass.
 *
 * The lease is the capability: holding one is what lets the exchange run an
 * effect in that partition, and releasing it is what lets the host reclaim it.
 * There is no way to run in a partition without holding a lease on it.
 */
export interface PartitionLease<R> {
  readonly key: PartitionKey;
  readonly runPromise: <A, E>(effect: Effect.Effect<A, E, R>) => Promise<A>;
  readonly release: () => void;
}

/** What the exchange needs from the host, and nothing more. */
export interface ExchangeSession {
  /** Source candidates: the workspace partitions this host currently holds open. */
  readonly openSources: () => readonly PartitionKey[];
  /**
   * Every workspace partition this host has opened, open or not.
   *
   * Registration reads this rather than the open set, because a workspace that
   * has just been idled out is still a source — and the pass that would have
   * registered it may not have run before it closed.
   */
  readonly knownSources: () => readonly PartitionKey[];
  /** The host's clock, so a scheduling policy is driven rather than awaited. */
  readonly now: () => number;
  readonly leaseWorkspace: (
    workspaceId: string,
  ) => PartitionLease<ApplicationServices | StreamGateway> | HostFailure;
  readonly leaseUser: (userId: string) => PartitionLease<UserServices> | HostFailure;
  readonly leaseGlobal: () => PartitionLease<GlobalServices> | HostFailure;
}

/** One pass over one source, as an operator reads it. */
export interface ExchangePassReport {
  readonly exchange: string;
  readonly source: PartitionKey;
  /** Records read from the source in this pass. */
  readonly scanned: number;
  /** Rows written to destination partitions. */
  readonly applied: number;
  readonly destinations: number;
  readonly fromArrival: number;
  readonly toArrival: number;
  readonly failed: boolean;
  readonly detail?: string;
}

export interface ExchangePassOptions {
  /** Upper bound on records read from one source in one pass. */
  limit?: number;
  /**
   * How many *closed* registered sources one pass may reopen.
   *
   * Reopening cold storage costs a partition slot and a database handle, so it
   * is budgeted rather than unbounded. Zero restores B4's behaviour exactly:
   * only what the host already holds open is exchanged.
   */
  coldSources?: number;
}

/** How many closed sources a pass reopens when the caller says nothing. */
export const DEFAULT_COLD_SOURCES_PER_PASS = 2;

const isLease = <R>(value: PartitionLease<R> | HostFailure): value is PartitionLease<R> =>
  !("_tag" in value);

/**
 * One pass over the sources this host is responsible for.
 *
 * Responsibility is the registry, not the open set: every workspace the host
 * has ever opened stays a source, so an inbox converges whether or not anyone
 * is currently looking at the workspace that feeds it. Open sources are always
 * visited; a bounded number of cold ones are reopened per pass, least recently
 * exchanged first, so every registered source is reached within a bounded
 * number of passes rather than by luck.
 *
 * Registration and scheduling both run in the global partition, which the pass
 * already leases — so this adds no new cross-domain reach.
 */
export async function runExchange(
  session: ExchangeSession,
  options: ExchangePassOptions = {},
): Promise<readonly ExchangePassReport[]> {
  const open = session.openSources();
  const budget = options.coldSources ?? DEFAULT_COLD_SOURCES_PER_PASS;
  const scheduled = await scheduleSources(session, open, budget);

  const reports: ExchangePassReport[] = [];
  for (const source of scheduled) {
    reports.push(await runExchangePass(session, source, options));
  }
  await recordVisits(session, scheduled);
  return reports;
}

/**
 * The sources this pass will visit.
 *
 * A failure to reach the registry is not a reason to exchange nothing: the open
 * sources are still sources, and refusing them would turn a bookkeeping outage
 * into a stalled product. The pass degrades to B4's behaviour instead.
 */
async function scheduleSources(
  session: ExchangeSession,
  open: readonly PartitionKey[],
  coldBudget: number,
): Promise<readonly PartitionKey[]> {
  const global = session.leaseGlobal();
  if (!isLease(global)) return open;
  try {
    const now = session.now();
    await global.runPromise(registerSources(session.knownSources(), now)).catch(() => undefined);
    if (coldBudget <= 0) return open;
    const registered = await global.runPromise(listSources()).catch(() => []);
    const cold: PartitionKey[] = [];
    for (const source of registered) {
      if (cold.length >= coldBudget) break;
      if (source.key.kind !== "workspace") continue;
      if (open.some((candidate) => partitionKeyEquals(candidate, source.key))) continue;
      cold.push(source.key);
    }
    return [...open, ...cold];
  } finally {
    global.release();
  }
}

/** Record that every visited source has just been looked at. */
async function recordVisits(
  session: ExchangeSession,
  visited: readonly PartitionKey[],
): Promise<void> {
  if (visited.length === 0) return;
  const global = session.leaseGlobal();
  if (!isLease(global)) return;
  try {
    const now = session.now();
    for (const source of visited) {
      await global.runPromise(touchSource(source, now)).catch(() => undefined);
    }
  } finally {
    global.release();
  }
}

const registerSources = (keys: readonly PartitionKey[], atMs: number) =>
  Effect.gen(function* () {
    const registry = yield* ExchangeSourceRegistry;
    yield* registry.register(keys, atMs);
  });

const listSources = () =>
  Effect.gen(function* () {
    const registry = yield* ExchangeSourceRegistry;
    return yield* registry.list();
  });

const touchSource = (key: PartitionKey, atMs: number) =>
  Effect.gen(function* () {
    const registry = yield* ExchangeSourceRegistry;
    yield* registry.touch(key, atMs);
  });

/** One pass over one source. Every failure is a report, never a throw. */
export async function runExchangePass(
  session: ExchangeSession,
  source: PartitionKey,
  options: ExchangePassOptions = {},
): Promise<ExchangePassReport> {
  const exchange = assignmentInbox.name;
  const held: { release: () => void }[] = [];
  const report = (
    fields: Partial<ExchangePassReport> & Pick<ExchangePassReport, "failed">,
  ): ExchangePassReport => ({
    exchange,
    source,
    scanned: 0,
    applied: 0,
    destinations: 0,
    fromArrival: 0,
    toArrival: 0,
    ...fields,
  });

  try {
    const global = session.leaseGlobal();
    if (!isLease(global)) return report({ failed: true, detail: describeFailure(global) });
    held.push(global);

    const cursor = await global.runPromise(readCursor(exchange, source));
    const from = cursor.arrival;

    const workspace = session.leaseWorkspace(source.id);
    if (!isLease(workspace)) return report({ failed: true, detail: describeFailure(workspace) });
    held.push(workspace);

    const page = await workspace.runPromise(
      readAssignmentActivity(source.id, from, options.limit ?? EXCHANGE_SOURCE_PAGE_LIMIT),
    );

    // Placement is checked for the whole page before one row is written, so a
    // single mis-keyed record refuses the pass rather than leaving the inbox
    // half-applied against a cursor that already moved.
    const planned = plan(source, page.records);
    if (planned instanceof ExchangeKeyMismatch) {
      return report({
        failed: true,
        scanned: page.records.length,
        fromArrival: from,
        toArrival: from,
        detail: `${planned._tag}: ${planned.side}/${planned.keyField}: ${planned.detail}`,
      });
    }

    let applied = 0;
    for (const [userId, rows] of planned) {
      const user = session.leaseUser(userId);
      if (!isLease(user)) {
        return report({
          failed: true,
          scanned: page.records.length,
          fromArrival: from,
          toArrival: from,
          detail: describeFailure(user),
        });
      }
      held.push(user);
      applied += await user.runPromise(applyInbox(userId, rows));
    }

    // Only now, with every destination written, does the position move.
    const advanced: ExchangeCursor = {
      ...cursor,
      arrival: page.arrival,
      applied: cursor.applied + applied,
    };
    await global.runPromise(writeCursor(advanced));

    return report({
      failed: false,
      scanned: page.records.length,
      applied,
      destinations: planned.size,
      fromArrival: from,
      toArrival: page.arrival,
    });
  } catch (cause) {
    return report({ failed: true, detail: describeCause(cause) });
  } finally {
    for (const lease of held.reverse()) lease.release();
  }
}

/**
 * Group one page's records by destination, checking placement as it goes.
 *
 * Returns the first mismatch rather than a partial plan: the caller must be
 * able to refuse the whole page, and a plan missing one record would look like
 * a page that simply had fewer of them.
 */
function plan(
  source: PartitionKey,
  records: readonly AssignmentActivity[],
): Map<string, InboxRow[]> | ExchangeKeyMismatch {
  const grouped = new Map<string, InboxRow[]>();
  for (const record of records) {
    const from = assignmentInbox.sourceKey(record);
    if (from instanceof ExchangeKeyMismatch) return from;
    if (!partitionKeyEquals(from, source)) {
      return new ExchangeKeyMismatch({
        exchange: assignmentInbox.name,
        side: "source",
        domain: source.kind,
        keyField: assignmentInbox.source.keyField ?? "",
        detail: `record is placed at ${partitionKeyString(from)}, read from ${partitionKeyString(source)}`,
      });
    }
    const to = assignmentInbox.destinationKey(record);
    if (to instanceof ExchangeKeyMismatch) return to;
    const row = assignmentInbox.rowFor(record, to);
    if (row instanceof ExchangeKeyMismatch) return row;
    const rows = grouped.get(to.id);
    if (rows === undefined) grouped.set(to.id, [row]);
    else rows.push(row);
  }
  return grouped;
}

const readCursor = Effect.fn("Exchange.readCursor")(function* (
  exchange: string,
  source: PartitionKey,
) {
  const cursors = yield* ExchangeCursorStore;
  return yield* cursors.read(exchange, assignmentInbox.version, source);
});

const writeCursor = Effect.fn("Exchange.writeCursor")(function* (cursor: ExchangeCursor) {
  const cursors = yield* ExchangeCursorStore;
  yield* cursors.advance(cursor);
});

/** A refused lease, as the pass reports it. The host's own translation, reused. */
function describeFailure(failure: HostFailure): string {
  const reported = hostFailureReport(failure);
  return `${failure._tag}: ${reported.detail}`;
}

function describeCause(cause: unknown): string {
  if (cause instanceof Error && cause.message.length > 0) return cause.message;
  return String(cause);
}
