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
  type StreamReadError,
} from "@streamsy/experimental/effect";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { OutboxStore, type OutboxDraft } from "@streamsy/effect-sink";
import { Clock, DateTime, Effect, Layer } from "effect";
import type {
  AssignIssueRequest,
  CreateIssueRequest,
  ChangeStatusRequest,
  LabelMembershipRequest,
} from "../shared/api.ts";
import type { CatalogUpsertRequest } from "../shared/api.ts";
import { catalog, decodeCatalogRow, type CatalogCollection } from "../domain/catalog.ts";
import {
  assignmentNotifications,
  boardIssues,
  boardLabelCounts,
  streamNames,
} from "../domain/declaration.ts";
import type {
  IssueEvent,
  IssueLabelEvent,
  IssueLabelRow,
  IssueRow,
  IssueStatus,
} from "../domain/issue.ts";
import { AppConfig } from "./config.ts";
import {
  appendFact,
  CommandProducers,
  encodeIssueFact,
  encodeLabelFact,
  hashCommandIntent,
  intentFromEvent,
  intentFromLabelEvent,
  membershipTarget,
  type CommandIntent,
} from "./commands.ts";
import {
  AppendRejected,
  CommandContention,
  CommandIdConflict,
  CommandRecoveryExhausted,
  InvalidRequest,
  SourcePoison,
  UnknownIssue,
  UnknownLabel,
} from "./errors.ts";
import {
  scanCanonicalIssueSource,
  scanCanonicalLabelSource,
  type CanonicalSource,
} from "./command-reconciliation.ts";
import { advance, type MaintenanceReport } from "./maintenance.ts";
import { assignmentDrafts, drainAssignments, NotificationTarget } from "./notifications.ts";
import { IssueSink } from "./sink.ts";
import { IssueStore, type CommandReceipt } from "./store.ts";
import { ensureWorkspace, Streams, type WorkspaceBindings } from "./streams.ts";
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

/**
 * One catalog collection, brought to its durable tail and read back.
 *
 * The joined collections are caught up by the maintenance pass, which is also
 * what feeds them to the two graph products — so a collection endpoint and the
 * board that joins it can no longer disagree about how far the catalog has been
 * read. `metadata` is joined by nothing, so it is caught up here.
 */
export const listCatalog = Effect.fn("Application.listCatalog")(function* (
  workspaceId: string,
  collection: CatalogCollection,
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  const report =
    collection === "metadata"
      ? yield* catchUpStateSource(collection, workspaceId)
      : yield* advance(workspaceId).pipe(
          Effect.map(
            (pass) =>
              pass.catalog.find((entry) => entry.collection === collection) ?? {
                collection,
                workspaceId,
                checkpoint: undefined,
                folded: 0,
                changes: [],
              },
          ),
        );
  const rows = yield* store.stateRows(stateSourceId(collection), collection, workspaceId);
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

/**
 * The wire result of one membership command.
 *
 * It is deliberately not a {@link CommandResult}: a membership command is about
 * an (issue, label) pair, so reporting an `issueId` and an issue row would be
 * reporting the wrong subject. The two share the receipt, the ack and the
 * maintenance report, which is what a caller needs to reconcile a retry.
 */
export interface LabelCommandResult {
  readonly commandId: string;
  readonly workspaceId: string;
  readonly issueId: string;
  readonly labelId: string;
  readonly membershipId: string;
  readonly attached: boolean;
  readonly eventId: string;
  readonly sequence: number;
  readonly ack: { readonly stream: string; readonly offset: string };
  readonly reconciled: boolean;
  readonly maintenance: MaintenanceReport;
  readonly row: IssueLabelRow | undefined;
}

/** Attach one label to one issue: `LabelAttached` on the membership stream. */
export const attachLabel = Effect.fn("Application.attachLabel")(function* (
  workspaceId: string,
  issueId: string,
  request: LabelMembershipRequest,
) {
  return yield* labelCommand(workspaceId, issueId, request, "LabelAttached");
});

/** Detach one label from one issue: `LabelDetached` on the membership stream. */
export const detachLabel = Effect.fn("Application.detachLabel")(function* (
  workspaceId: string,
  issueId: string,
  request: LabelMembershipRequest,
) {
  return yield* labelCommand(workspaceId, issueId, request, "LabelDetached");
});

/**
 * The membership command path.
 *
 * Both the issue and the label are checked against maintained state before a
 * fact is appended, for the reason `assignIssue` checks the issue: a membership
 * between things that do not exist is not a durable fact, it is a typo. The
 * label must exist in the catalog because the label-count product joins it —
 * a membership naming an unknown label would count nothing and look like a bug
 * in the plan rather than a bad request.
 */
const labelCommand = Effect.fn("Application.labelCommand")(function* (
  workspaceId: string,
  issueId: string,
  request: LabelMembershipRequest,
  type: "LabelAttached" | "LabelDetached",
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);

  const known = (yield* store.rows(workspaceId)).some((row) => row.issueId === issueId);
  if (!known) return yield* new UnknownIssue({ issueId });
  const catalogLabels = yield* store.stateRows(stateSourceId("labels"), "labels", workspaceId);
  const labelKnown = catalogLabels.some(
    (row) => "labelId" in row && row.labelId === request.labelId,
  );
  if (!labelKnown) return yield* new UnknownLabel({ labelId: request.labelId });

  const membershipId = membershipTarget(issueId, request.labelId);
  const outcome = yield* runCommand(
    labelLane,
    {
      workspaceId,
      commandId: request.commandId,
      commandKind: type === "LabelAttached" ? "attach-label" : "detach-label",
      targetId: membershipId,
      payload: { issueId, labelId: request.labelId },
    },
    (sequence, occurredAt) => ({
      type,
      eventId: request.commandId,
      workspaceId,
      issueId,
      labelId: request.labelId,
      membershipId,
      sequence,
      occurredAt,
    }),
  );

  const rows = yield* store.membershipRows(workspaceId);
  const row = rows.find((candidate) => candidate.membershipId === membershipId);
  return {
    commandId: outcome.receipt.commandId,
    workspaceId,
    issueId,
    labelId: request.labelId,
    membershipId,
    attached: row?.attached ?? type === "LabelAttached",
    eventId: outcome.receipt.eventId,
    sequence: outcome.receipt.eventSequence,
    ack: {
      stream: streamNames.issueLabelEvents(workspaceId),
      offset: outcome.receipt.eventOffset,
    },
    reconciled: outcome.reconciled,
    maintenance: outcome.maintenance,
    row,
  } satisfies LabelCommandResult;
});

/**
 * Every membership this workspace maintains, attached or not.
 *
 * Detached memberships are returned rather than filtered out, because they are
 * what the relation actually holds: a reader that wants only live labels reads
 * `attached`, and one auditing what a label was ever on needs the rest.
 */
export const listIssueLabels = Effect.fn("Application.listIssueLabels")(function* (
  workspaceId: string,
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  return yield* store.membershipRows(workspaceId);
});

/** The maintained label counts, as the sink publishes them. */
export const listLabelCounts = Effect.fn("Application.listLabelCounts")(function* (
  workspaceId: string,
) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  return yield* store.labelCountRows(workspaceId);
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
  const countHead = yield* Effect.promise((signal) =>
    streams.client.stream(streamNames.labelCountState(workspaceId)).head({ signal }),
  );
  return {
    sink: boardIssues.name,
    route: boardIssues.compiledRoute.build({ workspaceId }),
    transport: boardIssues.protocol.transport,
    fallback: boardIssues.protocol.fallback,
    protocolVersion: boardIssues.protocol.sessionVersion,
    durableStateVersion: boardIssues.protocol.durableStateVersion,
    contractFingerprint: boardIssues.fingerprint,
    offset,
    /**
     * The second checked State product this workspace publishes.
     *
     * It is reported beside the board rather than on its own endpoint because a
     * consumer binding the workspace binds both, and one round trip that names
     * every live contract is what lets it check them together.
     */
    labelCounts: {
      sink: boardLabelCounts.name,
      route: boardLabelCounts.compiledRoute.build({ workspaceId }),
      contractFingerprint: boardLabelCounts.fingerprint,
      offset: countHead.status === "ok" ? (countHead.offset ?? "-1") : "-1",
    },
  };
});

/**
 * One canonical fact family, as the command path sees it.
 *
 * The tracker now has two — issue facts and membership facts — and they need
 * exactly the same acceptance protocol: a receipt lookup, a producer lane, an
 * expected-offset CAS against the tail, and recovery of a receipt from the
 * durable fact when one was lost. A second copy of that protocol would be a
 * second place for the idempotency argument to be wrong, so the lane is a
 * parameter instead.
 */
interface CommandLane<Event extends { readonly eventId: string; readonly sequence: number }> {
  readonly stream: (workspaceId: string) => string;
  readonly bind: (bindings: WorkspaceBindings, workspaceId: string) => StreamBinding;
  readonly scan: (
    workspaceId: string,
    commandId: string,
  ) => Effect.Effect<
    CanonicalSource<Event>,
    SourcePoison | CommandRecoveryExhausted | StreamReadError,
    Streams | ReadStreams
  >;
  readonly encode: (event: Event) => string;
  readonly intentOf: (event: Event) => CommandIntent;
}

const issueLane: CommandLane<IssueEvent> = {
  stream: streamNames.issueEvents,
  bind: (bindings, workspaceId) => bindings.issueEvents(workspaceId),
  scan: scanCanonicalIssueSource,
  encode: encodeIssueFact,
  intentOf: intentFromEvent,
};

const labelLane: CommandLane<IssueLabelEvent> = {
  stream: streamNames.issueLabelEvents,
  bind: (bindings, workspaceId) => bindings.issueLabelEvents(workspaceId),
  scan: scanCanonicalLabelSource,
  encode: encodeLabelFact,
  intentOf: intentFromLabelEvent,
};

/** One accepted command: the receipt, whether it was already accepted, and the pass that followed. */
interface AcceptedCommand<Event> {
  readonly receipt: CommandReceipt;
  readonly reconciled: boolean;
  readonly maintenance: MaintenanceReport;
  readonly event: Event | undefined;
}

const acceptedCommand = <Event>(
  receipt: CommandReceipt,
  reconciled: boolean,
  maintenance: MaintenanceReport,
  event: Event | undefined,
): AcceptedCommand<Event> => ({ receipt, reconciled, maintenance, event });

/**
 * The shared command path.
 *
 * `eventId` is the command id, so a retry that reaches the append — because its
 * receipt was lost — rebuilds a byte-identical fact rather than a second
 * version of the same intent.
 */
const runCommand = Effect.fn("Application.command")(function* <
  Event extends { readonly eventId: string; readonly sequence: number },
>(
  lane: CommandLane<Event>,
  intent: CommandIntent,
  build: (sequence: number, occurredAt: string) => Event,
  /**
   * The effect-sink deliveries this command's fact implies.
   *
   * They are written with the receipt, in one durable step, so an accepted
   * command and the effects it owes are decided together. Every recovery path
   * below re-derives them from the *durable* fact, so a receipt recovered from
   * the canonical source still enqueues exactly what the original append would
   * have — and the outbox absorbs the repeat if it already did.
   */
  deliveries: (event: Event) => readonly OutboxDraft[] = () => [],
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
    return acceptedCommand(existing, true, yield* advance(workspaceId), undefined);
  }

  const producer = yield* producers.forCommand(workspaceId, commandId);
  const occurredAt = yield* Clock.currentTimeMillis.pipe(
    Effect.map((millis) => DateTime.formatIso(DateTime.makeUnsafe(millis))),
  );
  const binding = lane.bind(streams.bindings, workspaceId);

  for (let attempt = 1; attempt <= COMMAND_CAS_ATTEMPTS; attempt += 1) {
    const source = yield* lane.scan(workspaceId, commandId);
    const recovered = yield* receiptFromSource(lane, intent, requestHash, source);
    if (recovered !== undefined) {
      yield* store.recordReceipt(recovered.receipt, deliveries(recovered.event));
      const maintenance = yield* advance(workspaceId);
      return acceptedCommand(recovered.receipt, true, maintenance, recovered.event);
    }

    const event = build(source.maxSequence + 1, occurredAt);
    const decision = yield* appendFact(binding, lane.encode(event), producer, source.tail).pipe(
      Effect.map((append) => ({ kind: "append" as const, append })),
      Effect.catchTag("StreamAppendError", (error) =>
        Effect.gen(function* () {
          const afterFailure = yield* lane.scan(workspaceId, commandId);
          const recoveredAfterFailure = yield* receiptFromSource(
            lane,
            intent,
            requestHash,
            afterFailure,
          );
          if (recoveredAfterFailure === undefined) return yield* error;
          return { kind: "receipt" as const, recovered: recoveredAfterFailure };
        }),
      ),
    );
    if (decision.kind === "receipt") {
      yield* store.recordReceipt(decision.recovered.receipt, deliveries(decision.recovered.event));
      const maintenance = yield* advance(workspaceId);
      return acceptedCommand(
        decision.recovered.receipt,
        true,
        maintenance,
        decision.recovered.event,
      );
    }
    if (decision.append.status === "contention") continue;
    if (decision.append.status === "reconciled") {
      const afterDuplicate = yield* lane.scan(workspaceId, commandId);
      const duplicate = yield* receiptFromSource(lane, intent, requestHash, afterDuplicate);
      if (duplicate === undefined) {
        return yield* new AppendRejected({ stream: binding.streamId, status: "duplicate-missing" });
      }
      yield* store.recordReceipt(duplicate.receipt, deliveries(duplicate.event));
      const maintenance = yield* advance(workspaceId);
      return acceptedCommand(duplicate.receipt, true, maintenance, duplicate.event);
    }

    const receipt = receiptFor(intent, requestHash, event, decision.append.offset);
    yield* store.recordReceipt(receipt, deliveries(event));
    const maintenance = yield* advance(workspaceId);
    return acceptedCommand(receipt, false, maintenance, event);
  }
  return yield* new CommandContention({ workspaceId, attempts: COMMAND_CAS_ATTEMPTS });
});

/** The issue fact family's command path, with the issue-shaped result. */
const command = Effect.fn("Application.issueCommand")(function* (
  intent: CommandIntent,
  build: (sequence: number, occurredAt: string) => IssueEvent,
  deliveries: (event: IssueEvent) => readonly OutboxDraft[] = () => [],
) {
  const outcome = yield* runCommand(issueLane, intent, build, deliveries);
  return yield* result(outcome.receipt, outcome.reconciled, outcome.maintenance);
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
interface RecoveredCommand<Event> {
  readonly receipt: CommandReceipt;
  readonly event: Event;
}

const receiptFromSource = Effect.fn("Application.receiptFromSource")(function* <
  Event extends { readonly eventId: string; readonly sequence: number },
>(
  lane: CommandLane<Event>,
  intent: CommandIntent,
  requestHash: string,
  source: CanonicalSource<Event>,
) {
  if (source.match === undefined) return undefined;
  const durableHash = yield* hashCommandIntent(lane.intentOf(source.match.event));
  if (durableHash !== requestHash) {
    return yield* new CommandIdConflict({
      workspaceId: intent.workspaceId,
      commandId: intent.commandId,
    });
  }
  return {
    receipt: receiptFor(intent, requestHash, source.match.event, source.match.offset),
    event: source.match.event,
  } satisfies RecoveredCommand<Event>;
});

function receiptFor(
  intent: CommandIntent,
  requestHash: string,
  event: { readonly eventId: string; readonly sequence: number },
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
    return {
      workspaceId,
      issues: existing.map((row) => row.issueId),
      labels: SEED_LABELS.map((label) => label.labelId),
      seeded: false,
    };
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

  /**
   * The catalog labels and the memberships that put them on the board.
   *
   * A seeded workspace opens onto live label counts for the same reason it
   * opens onto a board: a product surface that is only reachable after the
   * reader has invented data is a product surface nobody checks.
   */
  for (const label of SEED_LABELS) {
    yield* upsertCatalog(workspaceId, "labels", {
      key: label.labelId,
      value: {
        labelId: label.labelId,
        workspaceId,
        name: label.name,
        color: label.color,
        updatedAt: SEED_TIMESTAMP,
      },
    });
  }
  for (const membership of SEED_MEMBERSHIPS) {
    yield* attachLabel(workspaceId, membership.issueId, {
      commandId: `seed-${workspaceId}-${membership.issueId}-${membership.labelId}`,
      labelId: membership.labelId,
    });
  }

  return {
    workspaceId,
    issues: seeds.map((seed) => seed.issueId),
    labels: SEED_LABELS.map((label) => label.labelId),
    seeded: true,
  };
});

const SEED_TIMESTAMP = "2026-08-25T00:00:00.000Z";

const SEED_LABELS: readonly {
  readonly labelId: string;
  readonly name: string;
  readonly color: string;
}[] = [
  { labelId: "bug", name: "Bug", color: "#d64545" },
  { labelId: "docs", name: "Docs", color: "#4573d6" },
  { labelId: "infra", name: "Infra", color: "#3f9d6b" },
];

const SEED_MEMBERSHIPS: readonly { readonly issueId: string; readonly labelId: string }[] = [
  { issueId: "seed-plan", labelId: "docs" },
  { issueId: "seed-maintain", labelId: "infra" },
  { issueId: "seed-publish", labelId: "infra" },
  { issueId: "seed-scale", labelId: "bug" },
];
