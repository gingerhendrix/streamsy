/**
 * The board.
 *
 * Four columns, one card per maintained row, and nothing reconstructed from a
 * command response: every card comes from the TanStack DB collection that the
 * sink keeps synchronized. That is the point of the slice — open two windows
 * and they agree, because they are reading the same durable product.
 */
import { useLiveQuery } from "@tanstack/react-db";
import { useEffect, useMemo, useRef, useState } from "react";
import { BOARD_COLUMNS, type IssueRow, type IssueStatus } from "../domain/issue.ts";
import {
  changeStatus,
  createIssue,
  newCommandId,
  seedWorkspace,
  type CommandAck,
} from "./lib/api.ts";
import {
  createBoardConnection,
  sortRows,
  type BoardConnection,
  type SinkStatus,
} from "./lib/board-db.ts";

function workspaceFromLocation(): string {
  return new URLSearchParams(window.location.search).get("workspace") ?? "main";
}

export function App(): React.JSX.Element {
  const workspaceId = useMemo(workspaceFromLocation, []);
  const [status, setStatus] = useState<SinkStatus>({ kind: "connecting" });
  const [connection, setConnection] = useState<BoardConnection | undefined>(undefined);

  useEffect(() => {
    const opened = createBoardConnection({
      workspaceId,
      origin: window.location.origin,
      onStatus: setStatus,
    });
    setConnection(opened);
    opened.preload().catch((cause: unknown) => {
      setStatus({
        kind: "failed",
        error: cause instanceof Error ? cause : new Error(String(cause)),
      });
    });
    // The application owns the session, so the application closes it.
    return () => {
      opened.close();
      setConnection(undefined);
    };
  }, [workspaceId]);

  if (connection === undefined) {
    return <main className="shell">Opening the board…</main>;
  }
  return <Board workspaceId={workspaceId} connection={connection} status={status} />;
}

function Board(props: {
  readonly workspaceId: string;
  readonly connection: BoardConnection;
  readonly status: SinkStatus;
}): React.JSX.Element {
  const { data } = useLiveQuery((query) =>
    query.from({ issue: props.connection.db.collections.issues }),
  );
  // SAFETY: the collection is built from `boardCollections`, whose schema is the
  // declared `IssueRow`; StreamDB decodes every row through it before writing,
  // so the live query can only yield rows that schema accepted.
  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  const rows = sortRows((data ?? []) as readonly IssueRow[]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);

  const run = (work: () => Promise<CommandAck | void>) => {
    setBusy(true);
    setError(undefined);
    work()
      .catch((cause: unknown) => {
        setError(cause instanceof Error ? cause.message : String(cause));
      })
      .finally(() => {
        setBusy(false);
      });
  };

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
              issueId: `issue-${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`,
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

      <div className="columns">
        {BOARD_COLUMNS.map((column) => (
          <Column
            key={column.status}
            label={column.label}
            status={column.status}
            rows={rows.filter((row) => row.status === column.status)}
            busy={busy}
            onMove={(issueId, next) =>
              run(() =>
                changeStatus(props.workspaceId, issueId, {
                  commandId: newCommandId("move"),
                  status: next,
                }),
              )
            }
          />
        ))}
      </div>
    </main>
  );
}

function Column(props: {
  readonly label: string;
  readonly status: IssueStatus;
  readonly rows: readonly IssueRow[];
  readonly busy: boolean;
  readonly onMove: (issueId: string, status: IssueStatus) => void;
}): React.JSX.Element {
  return (
    <section className="column" data-status={props.status} aria-label={props.label}>
      <h2>
        {props.label} <span className="count">{props.rows.length}</span>
      </h2>
      <ul>
        {props.rows.map((row) => (
          <li key={row.issueId} className="card" data-issue={row.issueId}>
            <p className="title">{row.title}</p>
            <label>
              <span className="visually-hidden">Status for {row.title}</span>
              <select
                value={row.status}
                disabled={props.busy}
                onChange={(changed) => {
                  // SAFETY: every option this select renders comes from
                  // `BOARD_COLUMNS`, so its value is one of the declared statuses.
                  // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
                  props.onMove(row.issueId, changed.target.value as IssueStatus);
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
        ))}
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
            // SAFETY: the options are rendered from `BOARD_COLUMNS`, so the
            // selected value is one of the declared statuses.
            // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
            setStatus(changed.target.value as IssueStatus);
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
        <span className="rows">
          {props.status.error instanceof Error
            ? props.status.error.message
            : props.status.error._tag}
        </span>
      ) : undefined}
    </p>
  );
}
