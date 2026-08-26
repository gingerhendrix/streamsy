/**
 * Domain identities, key alignment, and the exchange's position domain.
 *
 * Everything here is a pure value: no host, no storage, no partition. If these
 * rules are right, then a mis-keyed record cannot reach durable state and an
 * exchange cannot resume from a position that belongs to something else — both
 * of which are properties of the *declaration*, not of the runtime that
 * carries it.
 */
import { describe, expect, test } from "bun:test";
import { defaultOffsetGenerator } from "@streamsy/core";
import {
  DOMAIN_DIRECTORIES,
  GLOBAL_PARTITION_ID,
  globalKey,
  isDomainId,
  parsePartitionKey,
  partitionKeyEquals,
  partitionKeyString,
  partitionSegments,
  userKey,
  workspaceKey,
} from "../domain/domains.ts";
import {
  assignmentInbox,
  decodeExchangeCursor,
  defineExchange,
  ExchangeKeyMismatch,
  EXCHANGE_CURSOR_DOMAIN,
  initialCursor,
  InvalidExchangeDeclaration,
  type AssignmentActivity,
} from "../domain/exchange.ts";
import { inboxIdOf } from "../domain/inbox.ts";

const activity = (overrides: Partial<AssignmentActivity> = {}): AssignmentActivity => ({
  workspaceId: "left",
  issueId: "issue-1",
  assigneeId: "ada",
  status: "todo",
  eventId: "assign-1",
  occurredAt: "2026-08-26T00:00:00.000Z",
  sequence: 3,
  arrival: 4,
  ...overrides,
});

describe("partition keys", () => {
  test("a key names a domain and an identity, and round-trips as a string", () => {
    for (const key of [workspaceKey("left"), userKey("ada"), globalKey()]) {
      expect(parsePartitionKey(partitionKeyString(key))).toEqual(key);
    }
    expect(partitionKeyString(workspaceKey("ada"))).not.toBe(partitionKeyString(userKey("ada")));
    expect(partitionKeyEquals(workspaceKey("ada"), userKey("ada"))).toBe(false);
  });

  test("a domain accepts exactly the ids it can key a partition by", () => {
    for (const kind of ["workspace", "user"] as const) {
      expect(isDomainId(kind, "left")).toBe(true);
      expect(isDomainId(kind, "a.b-c_1")).toBe(true);
      expect(isDomainId(kind, "../escape")).toBe(false);
      expect(isDomainId(kind, "left/right")).toBe(false);
      expect(isDomainId(kind, "")).toBe(false);
      expect(isDomainId(kind, "NO!")).toBe(false);
    }
    // The global domain is a singleton, so no other id keys a global partition.
    expect(isDomainId("global", GLOBAL_PARTITION_ID)).toBe(true);
    expect(isDomainId("global", "other")).toBe(false);
  });

  test("a malformed key string is not a key", () => {
    for (const value of ["", "left", "workspace:", ":left", "nothing:left", "workspace:../up"]) {
      expect(parsePartitionKey(value)).toBeUndefined();
    }
  });

  test("each domain owns a directory, and B3's workspace layout is unchanged", () => {
    expect(DOMAIN_DIRECTORIES.workspace).toBe("workspaces");
    expect(partitionSegments(workspaceKey("left"))).toEqual(["workspaces", "left"]);
    expect(partitionSegments(userKey("ada"))).toEqual(["users", "ada"]);
    expect(partitionSegments(globalKey())).toEqual(["global", GLOBAL_PARTITION_ID]);
  });
});

describe("exchange declarations", () => {
  const placement = { domain: "workspace" as const, keyField: "workspaceId", key: () => "left" };

  test("two sides in the same place are a view, not an exchange", () => {
    expect(() =>
      defineExchange({
        name: "test.same-placement",
        version: 1,
        source: placement,
        destination: placement,
        rowKeyField: "workspaceId",
        rowKey: () => "left",
        project: () => ({}),
      }),
    ).toThrow(InvalidExchangeDeclaration);
  });

  test("a keyed domain must name the field it is keyed by", () => {
    expect(() =>
      defineExchange({
        name: "test.unkeyed",
        version: 1,
        source: placement,
        destination: { domain: "user", keyField: null, key: () => "ada" },
        rowKeyField: "userId",
        rowKey: () => "ada",
        project: () => ({}),
      }),
    ).toThrow(InvalidExchangeDeclaration);
  });

  test("the singleton global domain must not name one", () => {
    expect(() =>
      defineExchange({
        name: "test.keyed-global",
        version: 1,
        source: placement,
        destination: { domain: "global", keyField: "globalId", key: () => "global" },
        rowKeyField: "globalId",
        rowKey: () => "global",
        project: () => ({}),
      }),
    ).toThrow(InvalidExchangeDeclaration);
  });

  test("the destination row must name the key it is placed by", () => {
    expect(() =>
      defineExchange({
        name: "test.unplaced-row",
        version: 1,
        source: placement,
        destination: { domain: "user", keyField: "assigneeId", key: () => "ada" },
        rowKeyField: "",
        rowKey: () => "ada",
        project: () => ({}),
      }),
    ).toThrow(InvalidExchangeDeclaration);
  });

  test("the tracker's exchange crosses two domains by two different fields", () => {
    expect(assignmentInbox.source.domain).toBe("workspace");
    expect(assignmentInbox.source.keyField).toBe("workspaceId");
    expect(assignmentInbox.destination.domain).toBe("user");
    expect(assignmentInbox.destination.keyField).toBe("assigneeId");
    expect(assignmentInbox.rowKeyField).toBe("userId");
  });
});

describe("key alignment", () => {
  test("a well-formed record names both of its partitions", () => {
    const record = activity();
    expect(assignmentInbox.sourceKey(record)).toEqual(workspaceKey("left"));
    expect(assignmentInbox.destinationKey(record)).toEqual(userKey("ada"));
  });

  test("a key the domain refuses is a typed mismatch, not a partition", () => {
    for (const [side, record] of [
      ["source", activity({ workspaceId: "../escape" })],
      ["destination", activity({ assigneeId: "NO!" })],
    ] as const) {
      const key =
        side === "source"
          ? assignmentInbox.sourceKey(record)
          : assignmentInbox.destinationKey(record);
      expect(key).toBeInstanceOf(ExchangeKeyMismatch);
      if (!(key instanceof ExchangeKeyMismatch)) throw new Error("expected a mismatch");
      expect(key.side).toBe(side);
      expect(key.exchange).toBe(assignmentInbox.name);
    }
  });

  test("a record that carries no key at all is a mismatch, not a partition named undefined", () => {
    const unkeyed = defineExchange<AssignmentActivity, { readonly userId: string }>({
      name: "test.unkeyed-record",
      version: 1,
      source: { domain: "workspace", keyField: "workspaceId", key: (r) => r.workspaceId },
      destination: { domain: "user", keyField: "assigneeId", key: () => undefined },
      rowKeyField: "userId",
      rowKey: (row) => row.userId,
      project: (record) => ({ userId: record.assigneeId }),
    });
    const mismatch = unkeyed.destinationKey(activity());
    expect(mismatch).toBeInstanceOf(ExchangeKeyMismatch);
    if (!(mismatch instanceof ExchangeKeyMismatch)) throw new Error("expected a mismatch");
    expect(mismatch.detail).toContain("carries no assigneeId");
  });

  test("a projected row must land where the record was routed", () => {
    const record = activity();
    const row = assignmentInbox.rowFor(record, userKey("ada"));
    if (row instanceof ExchangeKeyMismatch) throw new Error("expected a row");
    expect(row.userId).toBe("ada");
    expect(row.inboxId).toBe(inboxIdOf("left", "assign-1"));

    // The same record routed at another user is refused before it is written.
    const misplaced = assignmentInbox.rowFor(record, userKey("grace"));
    expect(misplaced).toBeInstanceOf(ExchangeKeyMismatch);
    if (!(misplaced instanceof ExchangeKeyMismatch)) throw new Error("expected a mismatch");
    expect(misplaced.side).toBe("row");
    expect(misplaced.keyField).toBe("userId");
  });

  test("one fact projects to one row, whenever it is projected", () => {
    const record = activity();
    expect(assignmentInbox.rowFor(record, userKey("ada"))).toEqual(
      assignmentInbox.rowFor({ ...record }, userKey("ada")),
    );
  });

  test("two workspaces cannot collide on one inbox row key", () => {
    expect(inboxIdOf("left", "assign-1")).not.toBe(inboxIdOf("right", "assign-1"));
  });
});

describe("the exchange position domain", () => {
  const cursor = initialCursor(assignmentInbox.name, assignmentInbox.version, workspaceKey("left"));

  test("an exchange cursor carries its own position domain", () => {
    expect(cursor.domain).toBe(EXCHANGE_CURSOR_DOMAIN);
    expect(decodeExchangeCursor({ ...cursor, arrival: 12 }).arrival).toBe(12);
  });

  test("a position with no domain, or another domain, does not decode", () => {
    const { domain, ...untagged } = cursor;
    expect(domain).toBe(EXCHANGE_CURSOR_DOMAIN);
    expect(() => decodeExchangeCursor(untagged)).toThrow();
    expect(() =>
      decodeExchangeCursor({ ...cursor, domain: "issue-tracker.something-else" }),
    ).toThrow();
  });

  test("an A4 history position is not an exchange position", () => {
    // `HistoryPosition` is `{ epoch, sequence }` over a plan identity. It shares
    // no field with a cursor, and neither decodes as the other.
    const historyPosition = { epoch: 0, sequence: 7 };
    expect(() => decodeExchangeCursor(historyPosition)).toThrow();
    expect(Object.keys(cursor)).not.toContain("epoch");
    expect(Object.keys(cursor)).not.toContain("sequence");
  });

  test("a native Durable Streams offset is not an exchange position", () => {
    let offset = defaultOffsetGenerator.initialOffset;
    for (let step = 0; step < 3; step += 1) offset = defaultOffsetGenerator.next(offset);
    expect(defaultOffsetGenerator.isValid(offset)).toBe(true);

    expect(() => decodeExchangeCursor(offset)).toThrow();
    expect(() => decodeExchangeCursor({ ...cursor, arrival: offset })).toThrow();
    // And the cursor's own position is a count, which the offset scheme rejects.
    expect(defaultOffsetGenerator.isValid(String(cursor.arrival))).toBe(false);
  });

  test("a cursor written by another version of the exchange is not resumed from", () => {
    expect(decodeExchangeCursor({ ...cursor, version: 2 }).version).toBe(2);
    // The store is what refuses it; the schema only proves the value is a
    // cursor. See `exchange-store.ts`, which checks the declared version.
    expect(assignmentInbox.version).toBe(1);
  });

  test("the exchange never names a stream offset or a store checkpoint", async () => {
    // A structural guard: the exchange's own modules must not reach for a
    // position from another domain. Reviewing this once is worth less than
    // asserting it, because the temptation returns with every new source.
    for (const path of ["../server/exchange.ts", "../server/exchange-source.ts"]) {
      const source = await Bun.file(new URL(path, import.meta.url)).text();
      const code = source.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, "");
      for (const forbidden of ["HistoryPosition", "loadCheckpoint", "sourceProgress", "offset:"]) {
        expect(code).not.toContain(forbidden);
      }
    }
  });
});
