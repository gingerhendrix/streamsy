/** Three-column board with pointer drag and a first-class keyboard path. */
import { useState } from "react";
import type { BoardRow, IssueStatus } from "../../shared/model.ts";
import { ISSUE_STATUSES, isIssueStatus } from "../../shared/model.ts";
import {
  initials,
  memberName,
  PRIORITY_LABELS,
  relativeTime,
  STATUS_LABELS,
} from "../lib/format.ts";
import type { CardFailure, CardSync } from "../lib/pending.ts";

export interface BoardProps {
  readonly rows: readonly BoardRow[];
  readonly loading: boolean;
  readonly now: number;
  readonly selectedIssueId: string | null;
  readonly syncOf: (issueId: string) => CardSync;
  /** The newest failure on this issue, carrying the command a retry replays. */
  readonly failureOf: (issueId: string) => CardFailure | undefined;
  readonly noteOf: (issueId: string) => string | undefined;
  readonly onOpen: (issueId: string) => void;
  readonly onStatusChange: (row: BoardRow, status: IssueStatus) => void;
  readonly onRetry: (commandId: string) => void;
  readonly onCreate: (title: string, status: IssueStatus) => void;
}

export function Board(props: BoardProps) {
  const [dragOver, setDragOver] = useState<IssueStatus | null>(null);

  return (
    <div className="board" data-testid="board">
      {ISSUE_STATUSES.map((status) => {
        const rows = props.rows.filter((row) => row.status === status);
        return (
          <section
            key={status}
            className={`column${dragOver === status ? " column-over" : ""}`}
            data-testid={`column-${status}`}
            aria-label={`${STATUS_LABELS[status]} column`}
            onDragOver={(event) => {
              event.preventDefault();
              setDragOver(status);
            }}
            onDragLeave={() => setDragOver((current) => (current === status ? null : current))}
            onDrop={(event) => {
              event.preventDefault();
              setDragOver(null);
              const issueId = event.dataTransfer.getData("text/issue-id");
              const row = props.rows.find((candidate) => candidate.issueId === issueId);
              if (row !== undefined && row.status !== status) props.onStatusChange(row, status);
            }}
          >
            <header className="column-head">
              <h2>{STATUS_LABELS[status]}</h2>
              <span className="count" data-testid={`count-${status}`}>
                {props.loading ? "—" : rows.length}
              </span>
            </header>

            <div className="column-body">
              {props.loading ? (
                <SkeletonCards />
              ) : rows.length === 0 ? (
                <p className="empty">{emptyMessage(status)}</p>
              ) : (
                rows.map((row) => (
                  <IssueCard
                    key={row.issueId}
                    row={row}
                    now={props.now}
                    selected={props.selectedIssueId === row.issueId}
                    sync={props.syncOf(row.issueId)}
                    failure={props.failureOf(row.issueId)}
                    note={props.noteOf(row.issueId)}
                    onOpen={props.onOpen}
                    onStatusChange={props.onStatusChange}
                    onRetry={props.onRetry}
                  />
                ))
              )}
              <NewIssue status={status} onCreate={props.onCreate} />
            </div>
          </section>
        );
      })}
    </div>
  );
}

function emptyMessage(status: IssueStatus): string {
  if (status === "backlog") return "Nothing queued. Add the next piece of work.";
  if (status === "in-progress") return "No work in flight right now.";
  return "Nothing finished yet in this project.";
}

function SkeletonCards() {
  return (
    <>
      {[0, 1, 2].map((index) => (
        <div key={index} className="card skeleton" aria-hidden="true">
          <span className="skeleton-line short" />
          <span className="skeleton-line" />
          <span className="skeleton-line tiny" />
        </div>
      ))}
    </>
  );
}

interface IssueCardProps {
  readonly row: BoardRow;
  readonly now: number;
  readonly selected: boolean;
  readonly sync: CardSync;
  readonly failure: CardFailure | undefined;
  readonly note: string | undefined;
  readonly onOpen: (issueId: string) => void;
  readonly onStatusChange: (row: BoardRow, status: IssueStatus) => void;
  readonly onRetry: (commandId: string) => void;
}

function IssueCard(props: IssueCardProps) {
  const { row } = props;
  return (
    <article
      className={`card${props.selected ? " card-selected" : ""}${
        props.sync === "failed" ? " card-failed" : ""
      }`}
      data-testid={`card-${row.issueId}`}
      data-sync={props.sync}
      draggable
      onDragStart={(event) => {
        event.dataTransfer.setData("text/issue-id", row.issueId);
        event.dataTransfer.effectAllowed = "move";
      }}
    >
      <button
        type="button"
        className="card-open"
        onClick={() => props.onOpen(row.issueId)}
        aria-label={`Open ${row.issueKey}: ${row.title}`}
      >
        <span className="card-top">
          <span className="issue-key">{row.issueKey}</span>
          <span className={`priority priority-${row.priority}`}>
            {PRIORITY_LABELS[row.priority]}
          </span>
        </span>
        <span className="card-title">{row.title}</span>
      </button>

      <footer className="card-foot">
        <span className="avatar" title={memberName(row.assigneeId)} aria-hidden="true">
          {initials(row.assigneeId)}
        </span>
        <span className="sr-only">{memberName(row.assigneeId)}</span>
        {row.commentCount > 0 && (
          <span className="meta" title={`${row.commentCount} comments`}>
            💬 {row.commentCount}
          </span>
        )}
        <span className="meta grow">{relativeTime(row.updatedAt, props.now)}</span>
        <label className="sr-only" htmlFor={`status-${row.issueId}`}>
          Status for {row.issueKey}
        </label>
        <select
          id={`status-${row.issueId}`}
          className="status-select"
          value={row.status}
          data-testid={`status-select-${row.issueId}`}
          onChange={(event) => {
            const { value } = event.target;
            if (isIssueStatus(value)) props.onStatusChange(row, value);
          }}
        >
          {ISSUE_STATUSES.map((status) => (
            <option key={status} value={status}>
              {STATUS_LABELS[status]}
            </option>
          ))}
        </select>
      </footer>

      {props.sync === "syncing" && (
        <p className="card-state">
          <span className="spinner" aria-hidden="true" /> Syncing
        </p>
      )}
      {/* Accepted, but the board does not cover it yet. Never `Synced`. */}
      {props.sync === "pending" && (
        <p className="card-state warn">
          <span className="spinner" aria-hidden="true" /> Pending projections
          {props.note !== undefined && <span className="meta">{props.note}</span>}
        </p>
      )}
      {props.sync === "synced" && <p className="card-state ok">Synced</p>}
      {props.sync === "failed" && props.failure !== undefined && (
        <p className="card-state bad">
          <span>{props.failure.message}</span>
          <button
            type="button"
            className="link"
            onClick={() => props.onRetry(props.failure!.commandId)}
          >
            Retry sync
          </button>
        </p>
      )}
    </article>
  );
}

function NewIssue(props: {
  readonly status: IssueStatus;
  readonly onCreate: (title: string, status: IssueStatus) => void;
}) {
  const [open, setOpen] = useState(false);
  const [title, setTitle] = useState("");

  if (!open) {
    return (
      <button
        type="button"
        className="add-issue"
        data-testid={`add-issue-${props.status}`}
        onClick={() => setOpen(true)}
      >
        + New issue
      </button>
    );
  }

  return (
    <form
      className="add-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (title.trim().length === 0) return;
        props.onCreate(title.trim(), props.status);
        setTitle("");
        setOpen(false);
      }}
    >
      <label className="sr-only" htmlFor={`new-issue-${props.status}`}>
        New issue title in {STATUS_LABELS[props.status]}
      </label>
      <input
        id={`new-issue-${props.status}`}
        data-testid={`new-issue-input-${props.status}`}
        autoFocus
        value={title}
        placeholder="Issue title"
        onChange={(event) => setTitle(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Escape") setOpen(false);
        }}
      />
      <div className="add-actions">
        <button type="submit" className="primary" disabled={title.trim().length === 0}>
          Create
        </button>
        <button type="button" className="ghost" onClick={() => setOpen(false)}>
          Cancel
        </button>
      </div>
    </form>
  );
}
