/**
 * The application.
 *
 * Two things are on screen and they are not the same kind of thing, which is
 * the whole point of the assembly:
 *
 * - The **board** and the **label counts** are checked State sinks. Each has a
 *   generated TanStack DB binding, a contract fingerprint and a resumable
 *   session, so two windows converge without polling and a reset rebuilds
 *   exactly what the server says. Nothing on screen is reconstructed from a
 *   command response.
 * - The **read models** — per-issue labels, the activity feed, the summary,
 *   notifications and the user inbox — are polled. Three of them could not be
 *   live today for a stated reason (the inbox lives in a partition with no
 *   durable stream storage; the feed and summary are their own sink kinds), and
 *   the panels say so rather than implying convergence they do not have.
 */
import { useLiveQuery } from "@tanstack/react-db";
import { DateTime } from "effect";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BOARD_COLUMNS, type IssueStatus } from "../domain/issue.ts";
import type { BoardIssuesRow } from "./generated/board-issues.ts";
import {
  attachLabel,
  changeStatus,
  createIssue,
  detachLabel,
  fetchInbox,
  fetchWorkspaceReadModels,
  newCommandId,
  newIssueId,
  seedWorkspace,
  type CommandAck,
  type WorkspaceReadModels,
} from "./lib/api.ts";
import type { InboxRow } from "../domain/inbox.ts";
import {
  createBoardConnection,
  sortRows,
  type BoardConnection,
  type SinkStatus,
} from "./lib/board-db.ts";
import {
  createLabelCountsConnection,
  sortLabelCounts,
  type LabelCountsConnection,
} from "./lib/label-counts-db.ts";

/** How often the polled read models refresh when nothing else happens. */
const READ_MODEL_INTERVAL_MS = 2_000;

function workspaceFromLocation(): string {
  return new URLSearchParams(window.location.search).get("workspace") ?? "main";
}

/** Which user's inbox this window watches. The inbox is a cross-workspace product. */
function userFromLocation(): string {
  return new URLSearchParams(window.location.search).get("user") ?? "ada";
}

export function App(): React.JSX.Element {
  const workspaceId = useMemo(workspaceFromLocation, []);
  const userId = useMemo(userFromLocation, []);
  const [status, setStatus] = useState<SinkStatus>({ kind: "connecting" });
  const [countStatus, setCountStatus] = useState<SinkStatus>({ kind: "connecting" });
  const [connection, setConnection] = useState<BoardConnection | undefined>(undefined);
  const [counts, setCounts] = useState<LabelCountsConnection | undefined>(undefined);

  useEffect(() => {
    const board = createBoardConnection({
      workspaceId,
      origin: window.location.origin,
      onStatus: setStatus,
    });
    const labelCounts = createLabelCountsConnection({
      workspaceId,
      origin: window.location.origin,
      onStatus: setCountStatus,
    });
    setConnection(board);
    setCounts(labelCounts);
    const failed = (cause: unknown) => {
      setStatus({
        kind: "failed",
        error: cause instanceof Error ? cause : new Error(String(cause)),
      });
    };
    board.preload().catch(failed);
    labelCounts.preload().catch(failed);
    return () => {
      board.close();
      labelCounts.close();
      setConnection(undefined);
      setCounts(undefined);
    };
  }, [workspaceId]);

  if (connection === undefined || counts === undefined) {
    return <main className="shell">Opening the board…</main>;
  }
  return (
    <Board
      workspaceId={workspaceId}
      userId={userId}
      connection={connection}
      counts={counts}
      status={status}
      countStatus={countStatus}
    />
  );
}

function Board(props: {
  readonly workspaceId: string;
  readonly userId: string;
  readonly connection: BoardConnection;
  readonly counts: LabelCountsConnection;
  readonly status: SinkStatus;
  readonly countStatus: SinkStatus;
}): React.JSX.Element {
  const { data } = useLiveQuery((query) =>
    query.from({ issue: props.connection.db.collections.issues }),
  );
  // SAFETY: the collection is built from `boardCollections`, whose schema is the
  // declared `IssueRow`; StreamDB decodes every row through it before writing,
  // so the live query can only yield rows that schema accepted.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  const rows = sortRows(data ?? []);
  const { data: countData } = useLiveQuery((query) =>
    query.from({ labelCount: props.counts.db.collections.labelCounts }),
  );
  // SAFETY: same argument as the board's rows, against the label-count sink's
  // own declared schema.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  const labelCounts = sortLabelCounts(countData ?? []);

  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);
  const [models, setModels] = useState<WorkspaceReadModels | undefined>(undefined);
  const [inbox, setInbox] = useState<readonly InboxRow[] | undefined>(undefined);
  const [refreshedAt, setRefreshedAt] = useState<string | undefined>(undefined);

  const { workspaceId, userId } = props;
  const refresh = useCallback(
    () =>
      Promise.all([fetchWorkspaceReadModels(workspaceId), fetchInbox(userId)]).then(
        ([next, rowsForUser]) => {
          setModels(next);
          setInbox(rowsForUser);
          setRefreshedAt(DateTime.formatIso(DateTime.nowUnsafe()));
        },
      ),
    [workspaceId, userId],
  );

  useEffect(() => {
    let cancelled = false;
    const tick = () => {
      refresh().catch((cause: unknown) => {
        if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
      });
    };
    tick();
    // oxlint-disable-next-line effecttsgo/global-timers -- React owns this browser polling lifecycle and the cleanup below clears the matching platform interval.
    const timer = setInterval(tick, READ_MODEL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [refresh]);

  /**
   * Every command refreshes the polled panels as soon as it is acknowledged.
   *
   * The live collections need no such nudge; these do, and doing it here rather
   * than waiting for the interval is what makes a label attach feel like one
   * action instead of two.
   */
  const run = (work: () => Promise<CommandAck | void>) => {
    setBusy(true);
    setError(undefined);
    work()
      .then(() => refresh())
      .catch((cause: unknown) => {
        setError(cause instanceof Error ? cause.message : String(cause));
      })
      .finally(() => {
        setBusy(false);
      });
  };

  const labelsByIssue = new Map<string, readonly string[]>();
  for (const membership of models?.issueLabels ?? []) {
    if (!membership.attached) continue;
    labelsByIssue.set(membership.issueId, [
      ...(labelsByIssue.get(membership.issueId) ?? []),
      membership.labelId,
    ]);
  }

  return (
    <main className="shell">
      <header className="topbar">
        <h1>Streamsy issue tracker</h1>
        <SyncBadge status={props.status} rows={rows.length} />
      </header>

      <CreateIssueForm
        busy={busy}
        onCreate={(title, status) =>
          run(() =>
            createIssue(props.workspaceId, {
              commandId: newCommandId("create"),
              issueId: newIssueId(),
              projectId: "streamsy",
              title,
              status,
            }),
          )
        }
      />

      {rows.length === 0 ? (
        <button
          type="button"
          className="seed"
          disabled={busy}
          onClick={() => {
            run(() => seedWorkspace(props.workspaceId));
          }}
        >
          Seed this workspace
        </button>
      ) : undefined}

      {error === undefined ? undefined : (
        <p className="error" role="alert">
          {error}
        </p>
      )}

      <div className="board-layout">
        <aside className="sidebar" aria-label="Board counts">
          <h2>Board counts</h2>
          <dl>
            {BOARD_COLUMNS.map((column) => (
              <div key={column.status}>
                <dt>{column.label}</dt>
                <dd data-count-status={column.status}>
                  {rows.filter((row) => row.status === column.status).length}
                </dd>
              </div>
            ))}
          </dl>

          <h2>
            Label counts <LiveDot status={props.countStatus} />
          </h2>
          <dl data-panel="label-counts">
            {labelCounts.length === 0 ? (
              <div>
                <dt>No labels</dt>
                <dd data-count-label="none">0</dd>
              </div>
            ) : (
              labelCounts.map((row) => (
                <div key={row.labelId}>
                  <dt>{row.labelName}</dt>
                  <dd data-count-label={row.labelId}>{row.issueCount}</dd>
                </div>
              ))
            )}
          </dl>

          <h2>Projects</h2>
          <dl>
            {[...new Set(rows.map((row) => row.projectId))].toSorted().map((projectId) => (
              <div key={projectId}>
                <dt>{projectId}</dt>
                <dd data-count-project={projectId}>
                  {rows.filter((row) => row.projectId === projectId).length}
                </dd>
              </div>
            ))}
          </dl>
        </aside>

        <div className="columns">
          {BOARD_COLUMNS.map((column) => (
            <Column
              key={column.status}
              label={column.label}
              status={column.status}
              rows={rows.filter((row) => row.status === column.status)}
              labelsByIssue={labelsByIssue}
              labelCatalog={models?.labelCatalog ?? []}
              busy={busy}
              onMove={(issueId, next) =>
                run(() =>
                  changeStatus(props.workspaceId, issueId, {
                    commandId: newCommandId("move"),
                    status: next,
                  }),
                )
              }
              onAttach={(issueId, labelId) =>
                run(() =>
                  attachLabel(props.workspaceId, issueId, {
                    commandId: newCommandId("attach"),
                    labelId,
                  }),
                )
              }
              onDetach={(issueId, labelId) =>
                run(() =>
                  detachLabel(props.workspaceId, issueId, {
                    commandId: newCommandId("detach"),
                    labelId,
                  }),
                )
              }
            />
          ))}
        </div>
      </div>

      <WorkspacePanels
        userId={props.userId}
        models={models}
        inbox={inbox}
        refreshedAt={refreshedAt}
      />
    </main>
  );
}

/**
 * The polled half of the application, in one place.
 *
 * Grouping them is not cosmetic: every panel here is a read model rather than a
 * checked sink, they all refresh on the same signal, and the footer says when
 * that last happened. A reader can tell at a glance which parts of this screen
 * converge on their own and which are a snapshot from a moment ago.
 */
function WorkspacePanels(props: {
  readonly userId: string;
  readonly models: WorkspaceReadModels | undefined;
  readonly inbox: readonly InboxRow[] | undefined;
  readonly refreshedAt: string | undefined;
}): React.JSX.Element {
  const summary = props.models?.summary;
  const notifications = props.models?.notifications;
  return (
    <section className="panels" aria-label="Workspace read models">
      <article data-panel="summary">
        <h2>Summary</h2>
        {summary === undefined ? (
          <p className="muted">Loading…</p>
        ) : (
          <dl>
            <div>
              <dt>Issues</dt>
              <dd data-summary="total">{summary.issues.total}</dd>
            </div>
            <div>
              <dt>Labels</dt>
              <dd data-summary="labels">{summary.catalog.labels}</dd>
            </div>
            <div>
              <dt>Plan</dt>
              <dd data-summary="plan-hash">{summary.planHash}</dd>
            </div>
          </dl>
        )}
      </article>

      <article data-panel="inbox">
        <h2>Inbox for {props.userId}</h2>
        <p className="muted">
          Cross-workspace, served by the user partition. Polled, not resumable.
        </p>
        <ul>
          {(props.inbox ?? []).slice(0, 8).map((row) => (
            <li key={row.inboxId} data-inbox={row.inboxId}>
              <span data-inbox-workspace={row.workspaceId}>{row.workspaceId}</span>
              {" · "}
              <span>{row.issueId}</span>
            </li>
          ))}
          {props.inbox !== undefined && props.inbox.length === 0 ? (
            <li className="muted" data-inbox="empty">
              Nothing assigned yet
            </li>
          ) : undefined}
        </ul>
      </article>

      <article data-panel="activity">
        <h2>Activity</h2>
        <p className="muted">The declared transition feed, in arrival order.</p>
        <ul>
          {(props.models?.activity ?? [])
            .slice(-8)
            .toReversed()
            .map((event, index) => (
              <li key={`${event.issueId}-${event.occurredAt}-${String(index)}`}>
                <span data-activity-change={event.change}>{event.change}</span>
                {" · "}
                <span>{event.title}</span>
                {" · "}
                <span>{event.status}</span>
              </li>
            ))}
        </ul>
      </article>

      <article data-panel="notifications">
        <h2>Notifications</h2>
        {notifications === undefined ? (
          <p className="muted">Loading…</p>
        ) : (
          <dl>
            <div>
              <dt>Pending</dt>
              <dd data-outbox="pending">{notifications.pending}</dd>
            </div>
            <div>
              <dt>Delivered</dt>
              <dd data-outbox="delivered">{notifications.delivered}</dd>
            </div>
            <div>
              <dt>Dead</dt>
              <dd data-outbox="dead">{notifications.dead}</dd>
            </div>
          </dl>
        )}
      </article>

      <p className="muted refreshed" data-refreshed={props.refreshedAt ?? ""}>
        {props.refreshedAt === undefined
          ? "Read models have not refreshed yet"
          : `Read models refreshed at ${props.refreshedAt}`}
      </p>
    </section>
  );
}

/** Whether a checked sink session is live, shown beside the product it feeds. */
function LiveDot(props: { readonly status: SinkStatus }): React.JSX.Element {
  return (
    <span className="dot" data-live={props.status.kind}>
      {props.status.kind === "live" ? "live" : props.status.kind}
    </span>
  );
}

function Column(props: {
  readonly label: string;
  readonly status: IssueStatus;
  readonly rows: readonly BoardIssuesRow[];
  readonly labelsByIssue: ReadonlyMap<string, readonly string[]>;
  readonly labelCatalog: readonly { readonly labelId: string; readonly name: string }[];
  readonly busy: boolean;
  readonly onMove: (issueId: string, status: IssueStatus) => void;
  readonly onAttach: (issueId: string, labelId: string) => void;
  readonly onDetach: (issueId: string, labelId: string) => void;
}): React.JSX.Element {
  return (
    <section className="column" data-status={props.status} aria-label={props.label}>
      <h2>
        {props.label} <span className="count">{props.rows.length}</span>
      </h2>
      <ul>
        {props.rows.map((row) => {
          const attached = props.labelsByIssue.get(row.issueId) ?? [];
          const available = props.labelCatalog.filter((label) => !attached.includes(label.labelId));
          return (
            <li key={row.issueId} className="card" data-issue={row.issueId}>
              <p className="title">{row.title}</p>
              <p className="metadata">
                <span data-project={row.projectId}>{row.projectId}</span>
                <span data-assignee={row.assignee}>
                  {row.assignee === "unassigned" ? "Unassigned" : row.assignee}
                </span>
              </p>
              <p className="labels">
                {attached.map((labelId) => (
                  <button
                    key={labelId}
                    type="button"
                    className="chip"
                    data-label={labelId}
                    disabled={props.busy}
                    title={`Remove ${labelId}`}
                    onClick={() => {
                      props.onDetach(row.issueId, labelId);
                    }}
                  >
                    {labelId} ×
                  </button>
                ))}
              </p>
              <label>
                <span className="visually-hidden">Add a label to {row.title}</span>
                <select
                  className="add-label"
                  value=""
                  disabled={props.busy || available.length === 0}
                  onChange={(changed) => {
                    if (changed.target.value.length === 0) return;
                    props.onAttach(row.issueId, changed.target.value);
                  }}
                >
                  <option value="">Add label…</option>
                  {available.map((label) => (
                    <option key={label.labelId} value={label.labelId}>
                      {label.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span className="visually-hidden">Status for {row.title}</span>
                <select
                  className="issue-status"
                  value={row.status}
                  disabled={props.busy}
                  onChange={(changed) => {
                    const next = BOARD_COLUMNS.find(
                      (column) => column.status === changed.target.value,
                    );
                    if (next !== undefined) props.onMove(row.issueId, next.status);
                  }}
                >
                  {BOARD_COLUMNS.map((column) => (
                    <option key={column.status} value={column.status}>
                      {column.label}
                    </option>
                  ))}
                </select>
              </label>
            </li>
          );
        })}
      </ul>
    </section>
  );
}

function CreateIssueForm(props: {
  readonly busy: boolean;
  readonly onCreate: (title: string, status: IssueStatus) => void;
}): React.JSX.Element {
  const [title, setTitle] = useState("");
  const [status, setStatus] = useState<IssueStatus>("backlog");
  const input = useRef<HTMLInputElement>(null);

  return (
    <form
      className="create"
      onSubmit={(submitted) => {
        submitted.preventDefault();
        const trimmed = title.trim();
        if (trimmed.length === 0) return;
        props.onCreate(trimmed, status);
        setTitle("");
        input.current?.focus();
      }}
    >
      <label>
        <span className="visually-hidden">New issue title</span>
        <input
          ref={input}
          name="title"
          placeholder="New issue"
          value={title}
          onChange={(changed) => {
            setTitle(changed.target.value);
          }}
        />
      </label>
      <label>
        <span className="visually-hidden">New issue status</span>
        <select
          name="status"
          value={status}
          onChange={(changed) => {
            const next = BOARD_COLUMNS.find((column) => column.status === changed.target.value);
            if (next !== undefined) setStatus(next.status);
          }}
        >
          {BOARD_COLUMNS.map((column) => (
            <option key={column.status} value={column.status}>
              {column.label}
            </option>
          ))}
        </select>
      </label>
      <button type="submit" disabled={props.busy || title.trim().length === 0}>
        Create issue
      </button>
    </form>
  );
}

function sinkErrorMessage(error: SinkStatus & { readonly kind: "failed" }): string {
  if (error.error instanceof Error) return error.error.message;
  const { _tag: tag } = error.error;
  return tag;
}

/** `Live` means the sink answered successfully for this session. */
function SyncBadge(props: {
  readonly status: SinkStatus;
  readonly rows: number;
}): React.JSX.Element {
  const label =
    props.status.kind === "live"
      ? "Live"
      : props.status.kind === "connecting"
        ? "Connecting"
        : props.status.kind === "resetting"
          ? "Rebuilding"
          : "Failed";
  return (
    <p className="badge" data-state={props.status.kind} data-rows={props.rows}>
      <span>{label}</span>
      <span className="rows">{props.rows} issues</span>
      {props.status.kind === "live" && props.status.offset !== undefined ? (
        <span className="offset" title={props.status.offset}>
          offset held
        </span>
      ) : undefined}
      {props.status.kind === "failed" ? (
        <span className="rows">{sinkErrorMessage(props.status)}</span>
      ) : undefined}
    </p>
  );
}
