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
import { Clock, Effect, Layer } from "effect";
import type { CreateIssueRequest, ChangeStatusRequest } from "../shared/api.ts";
import { boardIssues, issues, streamNames } from "../domain/declaration.ts";
import type { IssueEvent, IssueRow, IssueStatus } from "../domain/issue.ts";
import { AppConfig } from "./config.ts";
import { appendIssueEvent, CommandProducers } from "./commands.ts";
import { UnknownIssue } from "./errors.ts";
import { advance, type MaintenanceReport } from "./maintenance.ts";
import { IssueSink } from "./sink.ts";
import { IssueStore, type CommandReceipt } from "./store.ts";
import { ensureWorkspace, Streams } from "./streams.ts";

export type ApplicationServices =
  | AppConfig
  | AppendStreams
  | CommandProducers
  | IssueSink
  | IssueStore
  | ReadStreams
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
  return yield* command(
    workspaceId,
    request.commandId,
    request.issueId,
    (sequence, occurredAt) => ({
      type: "IssueCreated",
      eventId: request.commandId,
      workspaceId,
      issueId: request.issueId,
      sequence,
      occurredAt,
      title: request.title,
      projectId: request.projectId,
      status: request.status ?? "backlog",
    }),
  );
});

/** Move one issue between columns: `IssueStatusChanged`. */
export const changeStatus = Effect.fn("Application.changeStatus")(function* (
  workspaceId: string,
  issueId: string,
  request: ChangeStatusRequest,
) {
  const store = yield* IssueStore;
  const receipt = yield* store.receipt(request.commandId);
  if (receipt === undefined) {
    // A move only means something against an issue that exists, so the current
    // rows are read before the fact is appended rather than after it.
    yield* ensureWorkspace(workspaceId);
    yield* advance(workspaceId);
    const known = (yield* store.rows(workspaceId)).some((row) => row.issueId === issueId);
    if (!known) return yield* new UnknownIssue({ issueId });
  }
  return yield* command(workspaceId, request.commandId, issueId, (sequence, occurredAt) => ({
    type: "IssueStatusChanged",
    eventId: request.commandId,
    workspaceId,
    issueId,
    sequence,
    occurredAt,
    status: request.status,
  }));
});

/** The maintained rows, read from durable state and decoded through the view's schema. */
export const listIssues = Effect.fn("Application.listIssues")(function* (workspaceId: string) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  return yield* store.rows(workspaceId);
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
    route: boardIssues.route.replace(":workspaceId", workspaceId),
    transport: boardIssues.protocol.transport,
    fallback: boardIssues.protocol.fallback,
    scope: boardIssues.auth.value,
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
  workspaceId: string,
  commandId: string,
  issueId: string,
  build: (sequence: number, occurredAt: string) => IssueEvent,
) {
  const store = yield* IssueStore;
  const streams = yield* Streams;
  const producers = yield* CommandProducers;

  yield* ensureWorkspace(workspaceId);

  const existing = yield* store.receipt(commandId);
  if (existing !== undefined) {
    // Already accepted. Report the original acceptance and append nothing.
    const maintenance = yield* advance(workspaceId);
    return yield* result(existing, true, maintenance, workspaceId, issueId);
  }

  const producer = yield* producers.forCommand(commandId);
  const sequence = yield* store.nextSequence(workspaceId);
  const occurredAt = yield* Clock.currentTimeMillis.pipe(
    Effect.map((millis) => new Date(millis).toISOString()),
  );
  const event = build(sequence, occurredAt);

  const binding = streams.bindings.issueEvents(workspaceId);
  const appended = yield* appendIssueEvent(binding, event, producer);

  const receipt: CommandReceipt = {
    commandId,
    workspaceId,
    issueId,
    offset: appended.offset,
    eventId: event.eventId,
    sequence: event.sequence,
  };
  yield* store.recordReceipt(receipt);

  const maintenance = yield* advance(workspaceId);
  return yield* result(
    receipt,
    appended.status === "reconciled",
    maintenance,
    workspaceId,
    issueId,
  );
});

const result = Effect.fn("Application.commandResult")(function* (
  receipt: CommandReceipt,
  reconciled: boolean,
  maintenance: MaintenanceReport,
  workspaceId: string,
  issueId: string,
) {
  const store = yield* IssueStore;
  const rows = yield* store.rows(workspaceId);
  return {
    commandId: receipt.commandId,
    workspaceId,
    issueId,
    eventId: receipt.eventId,
    sequence: receipt.sequence,
    ack: { stream: streamNames.issueEvents(workspaceId), offset: receipt.offset },
    reconciled,
    maintenance,
    row: rows.find((row) => row.issueId === issueId),
  } satisfies CommandResult;
});

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
