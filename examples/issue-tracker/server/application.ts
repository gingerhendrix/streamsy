/**
 * The command and query workflows.
 *
 * Every operation is a description that declares what it needs and how it can
 * fail. The router calls these; nothing here knows about HTTP.
 *
 * The command shape is the same in both cases: resolve the command's producer
 * lane, append one canonical fact, record a receipt, run one maintenance pass,
 * and report the durable acknowledgement. A retried `commandId` short-circuits
 * on its receipt, and — if the receipt were ever lost — the producer lane still
 * reconciles the append to the original offset, so the two mechanisms are
 * independent rather than one guarding the other.
 */
import {
  AppendStreams,
  AppendStreamsLive,
  ReadStreams,
  ReadStreamsLive,
} from "@streamsy/experimental/effect";
import { OutboxStore, type OutboxDraft } from "@streamsy/effect-sink";
import { Clock, Effect, Layer } from "effect";
import type { AssignIssueRequest, CreateIssueRequest, ChangeStatusRequest } from "../shared/api.ts";
import type { CatalogUpsertRequest } from "../shared/api.ts";
import { catalog, decodeCatalogRow, type CatalogCollection } from "../domain/catalog.ts";
import {
  assignmentNotifications,
  boardIssues,
  issues,
  streamNames,
} from "../domain/declaration.ts";
import type { IssueEvent, IssueRow, IssueStatus } from "../domain/issue.ts";
import { AppConfig } from "./config.ts";
import {
  appendIssueEvent,
  CommandProducers,
  hashCommandIntent,
  intentFromEvent,
  type CommandIntent,
} from "./commands.ts";
import {
  AppendRejected,
  CommandContention,
  CommandIdConflict,
  InvalidRequest,
  UnknownIssue,
} from "./errors.ts";
import { scanCanonicalIssueSource, type CanonicalIssueSource } from "./command-reconciliation.ts";
import { advance, type MaintenanceReport } from "./maintenance.ts";
import { assignmentDrafts, drainAssignments, NotificationTarget } from "./notifications.ts";
import { IssueSink } from "./sink.ts";
import { IssueStore, type CommandReceipt } from "./store.ts";
import { ensureWorkspace, Streams } from "./streams.ts";
import {
  catchUpStateSource,
  StateSourceProtocol,
  stateSourceBinding,
  stateSourceId,
} from "./state-ingestion.ts";

export type ApplicationServices =
  | AppConfig
  | AppendStreams
  | CommandProducers
  | IssueSink
  | IssueStore
  | NotificationTarget
  | OutboxStore
  | ReadStreams
  | StateSourceProtocol
  | Streams;

/** The mesh capabilities this application uses, in one layer. */
export const MeshLayer: Layer.Layer<AppendStreams | ReadStreams> = Layer.mergeAll(
  AppendStreamsLive,
  ReadStreamsLive,
);

export interface CommandResult {
  readonly commandId: string;
  readonly workspaceId: string;
  readonly issueId: string;
  readonly eventId: string;
  readonly sequence: number;
  readonly ack: { readonly stream: string; readonly offset: string };
  readonly reconciled: boolean;
  readonly maintenance: MaintenanceReport;
  readonly row: IssueRow | undefined;
}

const COMMAND_CAS_ATTEMPTS = 8;

export const health = Effect.fn("Application.health")(function* () {
  const config = yield* AppConfig;
  return {
    status: "ok" as const,
    deployment: config.deployment,
    schemaVersion: config.schemaVersion,
    view: issues.name,
    planHash: config.planHash,
  };
});

/** Create one issue: `IssueCreated` onto the workspace's canonical stream. */
export const createIssue = Effect.fn("Application.createIssue")(function* (
  workspaceId: string,
  request: CreateIssueRequest,
) {
  yield* ensureDefaultProject(workspaceId);
  const intent: CommandIntent = {
    workspaceId,
    commandId: request.commandId,
    commandKind: "create-issue",
    targetId: request.issueId,
    payload: {
      projectId: request.projectId,
      status: request.status ?? "backlog",
      title: request.title,
    },
  };
  return yield* command(intent, (sequence, occurredAt) => ({
    type: "IssueCreated",
    eventId: request.commandId,
    workspaceId,
    issueId: request.issueId,
    sequence,
    occurredAt,
    title: request.title,
    projectId: request.projectId,
    status: request.status ?? "backlog",
  }));
});

/** Move one issue between columns: `IssueStatusChanged`. */
export const changeStatus = Effect.fn("Application.changeStatus")(function* (
  workspaceId: string,
  issueId: string,
  request: ChangeStatusRequest,
) {
  const store = yield* IssueStore;
  const receipt = yield* store.receipt(workspaceId, request.commandId);
  if (receipt === undefined) {
    // A move only means something against an issue that exists, so the current
    // rows are read before the fact is appended rather than after it.
    yield* ensureWorkspace(workspaceId);
    yield* advance(workspaceId);
    const known = (yield* store.rows(workspaceId)).some((row) => row.issueId === issueId);
    if (!known) return yield* new UnknownIssue({ issueId });
  }
  return yield* command(
    {
      workspaceId,
      commandId: request.commandId,
      commandKind: "change-status",
      targetId: issueId,
      payload: { status: request.status },
    },
    (sequence, occurredAt) => ({
      type: "IssueStatusChanged",
      eventId: request.commandId,
      workspaceId,
      issueId,
      sequence,
      occurredAt,
      status: request.status,
    }),
  );
});

/**
 * Put one issue on an assignee: `IssueAssigned`.
 *
 * The maintained row is read before the fact is appended, for two reasons that
 * are both about honesty. Assigning an issue that does not exist is a 404, not
 * a durable fact. And the notification the effect sink will deliver carries the
 * issue's title and status, which live on the row rather than on the event, so
 * they are read from the same row the fact is about.
 */
export const assignIssue = Effect.fn("Application.assignIssue")(function* (
  workspaceId: string,
  issueId: string,
  request: AssignIssueRequest,
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  const current = (yield* store.rows(workspaceId)).find((row) => row.issueId === issueId);
  if (current === undefined) return yield* new UnknownIssue({ issueId });

  const enqueuedAtMs = yield* Clock.currentTimeMillis;
  return yield* command(
    {
      workspaceId,
      commandId: request.commandId,
      commandKind: "assign-issue",
      targetId: issueId,
      payload: { assigneeId: request.assigneeId, status: current.status },
    },
    (sequence, occurredAt) => ({
      type: "IssueAssigned",
      eventId: request.commandId,
      workspaceId,
      issueId,
      sequence,
      occurredAt,
      status: current.status,
      assigneeId: request.assigneeId,
    }),
    (event) => assignmentDrafts(event, current, enqueuedAtMs),
  );
});

/** The durable delivery state of one workspace's assignment notifications. */
export const listNotifications = Effect.fn("Application.listNotifications")(function* (
  workspaceId: string,
) {
  const outbox = yield* OutboxStore;
  const target = yield* NotificationTarget;
  const entries = yield* outbox.list(assignmentNotifications.name, workspaceId);
  return {
    sink: assignmentNotifications.name,
    contractFingerprint: assignmentNotifications.fingerprint,
    entries,
    notified: yield* target.accepted(workspaceId),
  };
});

/**
 * Attempt every due delivery in one workspace's lane.
 *
 * Delivery is deliberately not part of the command path. A notifier that is
 * down must not fail a command, delay a maintenance pass, or hold up the
 * board's publication, so draining is its own operation with its own report.
 */
export const drainNotifications = Effect.fn("Application.drainNotifications")(function* (
  workspaceId: string,
) {
  return yield* drainAssignments(workspaceId);
});

/** The maintained rows, read from durable state and decoded through the view's schema. */
export const listIssues = Effect.fn("Application.listIssues")(function* (workspaceId: string) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  return yield* store.rows(workspaceId);
});

export const listCatalog = Effect.fn("Application.listCatalog")(function* (
  workspaceId: string,
  collection: CatalogCollection,
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  const report = yield* catchUpStateSource(collection, workspaceId);
  const rows = yield* store.stateRows(stateSourceId(collection), collection, workspaceId);
  if ((collection === "projects" || collection === "users") && report.changes.length > 0) {
    const board = yield* store.maintainBoard(workspaceId, [
      {
        sourceId: `issue-tracker.${collection}`,
        changes: report.changes.map((change) => JSON.parse(JSON.stringify(change))),
      },
    ]);
    const sink = yield* IssueSink;
    yield* sink.publish(workspaceId, board.changes);
  }
  return { report, rows };
});

export const upsertCatalog = Effect.fn("Application.upsertCatalog")(function* (
  workspaceId: string,
  collection: CatalogCollection,
  request: CatalogUpsertRequest,
) {
  const streams = yield* Streams;
  const appends = yield* AppendStreams;
  yield* ensureWorkspace(workspaceId);
  const decoded = yield* Effect.try({
    try: () => decodeCatalogRow(collection, request.value),
    catch: (cause) =>
      InvalidRequest.of("value", cause instanceof Error ? cause.message : String(cause)),
  });
  if (decoded.key !== request.key) {
    return yield* InvalidRequest.of("key", `must equal row key ${decoded.key}`);
  }
  if (decoded.workspaceId !== workspaceId) {
    return yield* InvalidRequest.of("workspaceId", `must equal ${workspaceId}`);
  }
  const binding = stateSourceBinding(streams.bindings, collection, workspaceId);
  const appended = yield* appends.appendJsonBatch(binding, [
    {
      type: catalog[collection].type,
      key: request.key,
      value: request.value,
      headers: { operation: "upsert" },
    },
  ]);
  if (appended.status !== "appended" && appended.status !== "duplicate") {
    return yield* new AppendRejected({ stream: binding.streamId, status: appended.status });
  }
  return yield* listCatalog(workspaceId, collection);
});

const ensureDefaultProject = Effect.fn("Application.ensureDefaultProject")(function* (
  workspaceId: string,
) {
  const existing = yield* listCatalog(workspaceId, "projects");
  if (existing.rows.some((row) => "projectId" in row && row.projectId === "streamsy")) return;
  yield* upsertCatalog(workspaceId, "projects", {
    key: "streamsy",
    value: {
      projectId: "streamsy",
      workspaceId,
      key: "STR",
      name: "Streamsy",
      updatedAt: "2026-08-25T00:00:00.000Z",
    },
  });
});

/** The sink session contract: where the product lives and its current offset. */
export const sinkSession = Effect.fn("Application.sinkSession")(function* (workspaceId: string) {
  const streams = yield* Streams;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);

  const head = yield* Effect.promise((signal) =>
    streams.client.stream(streamNames.boardState(workspaceId)).head({ signal }),
  );
  const offset = head.status === "ok" ? (head.offset ?? "-1") : "-1";
  return {
    sink: boardIssues.name,
    route: boardIssues.compiledRoute.build({ workspaceId }),
    transport: boardIssues.protocol.transport,
    fallback: boardIssues.protocol.fallback,
    protocolVersion: boardIssues.protocol.sessionVersion,
    durableStateVersion: boardIssues.protocol.durableStateVersion,
    contractFingerprint: boardIssues.fingerprint,
    offset,
  };
});

/**
 * The shared command path.
 *
 * `eventId` is the command id, so a retry that reaches the append — because its
 * receipt was lost — rebuilds a byte-identical fact rather than a second
 * version of the same intent.
 */
const command = Effect.fn("Application.command")(function* (
  intent: CommandIntent,
  build: (sequence: number, occurredAt: string) => IssueEvent,
  /**
   * The effect-sink deliveries this command's fact implies.
   *
   * They are written with the receipt, in one durable step, so an accepted
   * command and the effects it owes are decided together. Every recovery path
   * below re-derives them from the *durable* fact, so a receipt recovered from
   * the canonical source still enqueues exactly what the original append would
   * have — and the outbox absorbs the repeat if it already did.
   */
  deliveries: (event: IssueEvent) => readonly OutboxDraft[] = () => [],
) {
  const store = yield* IssueStore;
  const streams = yield* Streams;
  const producers = yield* CommandProducers;

  const { workspaceId, commandId } = intent;
  yield* ensureWorkspace(workspaceId);
  const requestHash = yield* hashCommandIntent(intent);

  const existing = yield* store.receipt(workspaceId, commandId);
  if (existing !== undefined) {
    if (existing.requestHash !== requestHash) {
      return yield* new CommandIdConflict({ workspaceId, commandId });
    }
    // Already accepted. Report the original acceptance and append nothing.
    const maintenance = yield* advance(workspaceId);
    return yield* result(existing, true, maintenance);
  }

  const producer = yield* producers.forCommand(workspaceId, commandId);
  const occurredAt = yield* Clock.currentTimeMillis.pipe(
    Effect.map((millis) => new Date(millis).toISOString()),
  );
  const binding = streams.bindings.issueEvents(workspaceId);

  for (let attempt = 1; attempt <= COMMAND_CAS_ATTEMPTS; attempt += 1) {
    const source = yield* scanCanonicalIssueSource(workspaceId, commandId);
    const recovered = yield* receiptFromSource(intent, requestHash, source);
    if (recovered !== undefined) {
      yield* store.recordReceipt(recovered.receipt, deliveries(recovered.event));
      const maintenance = yield* advance(workspaceId);
      return yield* result(recovered.receipt, true, maintenance);
    }

    const event = build(source.maxSequence + 1, occurredAt);
    const decision = yield* appendIssueEvent(binding, event, producer, source.tail).pipe(
      Effect.map((append) => ({ kind: "append" as const, append })),
      Effect.catchTag("StreamAppendError", (error) =>
        Effect.gen(function* () {
          const afterFailure = yield* scanCanonicalIssueSource(workspaceId, commandId);
          const recoveredAfterFailure = yield* receiptFromSource(intent, requestHash, afterFailure);
          if (recoveredAfterFailure === undefined) return yield* error;
          return { kind: "receipt" as const, recovered: recoveredAfterFailure };
        }),
      ),
    );
    if (decision.kind === "receipt") {
      yield* store.recordReceipt(decision.recovered.receipt, deliveries(decision.recovered.event));
      const maintenance = yield* advance(workspaceId);
      return yield* result(decision.recovered.receipt, true, maintenance);
    }
    if (decision.append.status === "contention") continue;
    if (decision.append.status === "reconciled") {
      const afterDuplicate = yield* scanCanonicalIssueSource(workspaceId, commandId);
      const duplicate = yield* receiptFromSource(intent, requestHash, afterDuplicate);
      if (duplicate === undefined) {
        return yield* new AppendRejected({ stream: binding.streamId, status: "duplicate-missing" });
      }
      yield* store.recordReceipt(duplicate.receipt, deliveries(duplicate.event));
      const maintenance = yield* advance(workspaceId);
      return yield* result(duplicate.receipt, true, maintenance);
    }

    const receipt = receiptFor(intent, requestHash, event, decision.append.offset);
    yield* store.recordReceipt(receipt, deliveries(event));
    const maintenance = yield* advance(workspaceId);
    return yield* result(receipt, false, maintenance);
  }
  return yield* new CommandContention({ workspaceId, attempts: COMMAND_CAS_ATTEMPTS });
});

const result = Effect.fn("Application.commandResult")(function* (
  receipt: CommandReceipt,
  reconciled: boolean,
  maintenance: MaintenanceReport,
) {
  const store = yield* IssueStore;
  const rows = yield* store.rows(receipt.workspaceId);
  return {
    commandId: receipt.commandId,
    workspaceId: receipt.workspaceId,
    issueId: receipt.targetId,
    eventId: receipt.eventId,
    sequence: receipt.eventSequence,
    ack: {
      stream: streamNames.issueEvents(receipt.workspaceId),
      offset: receipt.eventOffset,
    },
    reconciled,
    maintenance,
    row: rows.find((row) => row.issueId === receipt.targetId),
  } satisfies CommandResult;
});

/** A receipt recovered from the durable source, together with the fact that proves it. */
interface RecoveredCommand {
  readonly receipt: CommandReceipt;
  readonly event: IssueEvent;
}

const receiptFromSource = Effect.fn("Application.receiptFromSource")(function* (
  intent: CommandIntent,
  requestHash: string,
  source: CanonicalIssueSource,
) {
  if (source.match === undefined) return undefined;
  const durableHash = yield* hashCommandIntent(intentFromEvent(source.match.event));
  if (durableHash !== requestHash) {
    return yield* new CommandIdConflict({
      workspaceId: intent.workspaceId,
      commandId: intent.commandId,
    });
  }
  return {
    receipt: receiptFor(intent, requestHash, source.match.event, source.match.offset),
    event: source.match.event,
  } satisfies RecoveredCommand;
});

function receiptFor(
  intent: CommandIntent,
  requestHash: string,
  event: IssueEvent,
  eventOffset: string,
): CommandReceipt {
  return {
    workspaceId: intent.workspaceId,
    commandId: intent.commandId,
    commandKind: intent.commandKind,
    targetId: intent.targetId,
    requestHash,
    eventId: event.eventId,
    eventSequence: event.sequence,
    eventOffset,
  };
}

/** A tiny seeded board, so a fresh workspace opens onto something. */
export const seedWorkspace = Effect.fn("Application.seedWorkspace")(function* (
  workspaceId: string,
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  const existing = yield* store.rows(workspaceId);
  if (existing.length > 0) {
    return { workspaceId, issues: existing.map((row) => row.issueId), seeded: false };
  }

  const seeds: readonly {
    readonly issueId: string;
    readonly title: string;
    readonly status: IssueStatus;
  }[] = [
    { issueId: "seed-plan", title: "Declare the issue view", status: "done" },
    {
      issueId: "seed-maintain",
      title: "Maintain rows from canonical facts",
      status: "in_progress",
    },
    { issueId: "seed-publish", title: "Publish the board through one stateSink", status: "todo" },
    {
      issueId: "seed-scale",
      title: "Grow the tracker into the flagship example",
      status: "backlog",
    },
  ];

  for (const seed of seeds) {
    yield* createIssue(workspaceId, {
      commandId: `seed-${workspaceId}-${seed.issueId}`,
      issueId: seed.issueId,
      projectId: "streamsy",
      title: seed.title,
      status: seed.status,
    });
  }
  return { workspaceId, issues: seeds.map((seed) => seed.issueId), seeded: true };
});
