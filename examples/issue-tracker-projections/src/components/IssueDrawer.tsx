/** Right-side detail drawer on desktop, full-screen sheet on mobile. */
import { useEffect, useRef, useState } from "react";
import type { IssueDetail, IssuePriority, IssueStatus, TeamMemberId } from "../../shared/model.ts";
import { ISSUE_PRIORITIES, ISSUE_STATUSES, isKnownMember, TEAM } from "../../shared/model.ts";
import { memberName, PRIORITY_LABELS, relativeTime, STATUS_LABELS } from "../lib/format.ts";
import type { CardSync } from "../lib/pending.ts";

export interface IssueDrawerProps {
  readonly detail: IssueDetail | undefined;
  readonly loading: boolean;
  readonly error: string | undefined;
  readonly note: string | undefined;
  readonly sync: CardSync;
  readonly now: number;
  readonly onClose: () => void;
  readonly onRename: (title: string) => void;
  readonly onStatus: (status: IssueStatus) => void;
  readonly onPriority: (priority: IssuePriority) => void;
  readonly onAssign: (assigneeId: TeamMemberId | null) => void;
  readonly onComment: (body: string) => void;
  readonly onRetry: () => void;
}

export function IssueDrawer(props: IssueDrawerProps) {
  const { detail } = props;
  const closeRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const [title, setTitle] = useState(detail?.title ?? "");
  const [comment, setComment] = useState("");

  useEffect(() => {
    closeRef.current?.focus();
  }, []);

  // Keep the field in step with durable state unless it is being edited.
  useEffect(() => {
    if (detail === undefined) return;
    const active = globalThis.document.activeElement;
    if (active instanceof HTMLInputElement && active.id === "issue-title") return;
    setTitle(detail.title);
  }, [detail]);

  return (
    <div
      className="drawer"
      role="dialog"
      aria-modal="false"
      aria-label={detail === undefined ? "Issue detail" : `${detail.issueKey} ${detail.title}`}
      data-testid="issue-drawer"
      ref={panelRef}
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          props.onClose();
        }
      }}
    >
      <header className="drawer-head">
        <span className="issue-key">{detail?.issueKey ?? "…"}</span>
        {props.sync === "syncing" && (
          <span className="chip">
            <span className="spinner" aria-hidden="true" /> Syncing
          </span>
        )}
        {props.sync === "pending" && (
          <span className="chip warn" title={props.note}>
            <span className="spinner" aria-hidden="true" /> Pending
          </span>
        )}
        {props.sync === "synced" && <span className="chip ok">Synced</span>}
        {props.sync === "failed" && (
          <span className="chip bad">
            Failed
            <button type="button" className="link" onClick={props.onRetry}>
              Retry sync
            </button>
          </span>
        )}
        <button
          type="button"
          className="ghost close"
          ref={closeRef}
          onClick={props.onClose}
          data-testid="close-drawer"
        >
          Close
        </button>
      </header>

      {props.error !== undefined && (
        <p className="banner bad" role="alert">
          {props.error}
        </p>
      )}

      {props.sync === "pending" && props.note !== undefined && (
        <p className="banner warn" data-testid="pending-note">
          {props.note}
        </p>
      )}

      {detail === undefined ? (
        <p className="empty">{props.loading ? "Loading issue…" : "Issue not found."}</p>
      ) : (
        <div className="drawer-body">
          <form
            className="field"
            onSubmit={(event) => {
              event.preventDefault();
              if (title.trim().length > 0 && title.trim() !== detail.title) {
                props.onRename(title.trim());
              }
              (event.currentTarget.querySelector("input") as HTMLInputElement | null)?.blur();
            }}
          >
            <label htmlFor="issue-title">Title</label>
            <input
              id="issue-title"
              data-testid="issue-title"
              value={title}
              onChange={(event) => setTitle(event.target.value)}
              onBlur={() => {
                if (title.trim().length > 0 && title.trim() !== detail.title) {
                  props.onRename(title.trim());
                } else {
                  setTitle(detail.title);
                }
              }}
            />
          </form>

          <div className="field-grid">
            <div className="field">
              <label htmlFor="issue-status">Status</label>
              <select
                id="issue-status"
                data-testid="issue-status"
                value={detail.status}
                onChange={(event) => props.onStatus(event.target.value as IssueStatus)}
              >
                {ISSUE_STATUSES.map((status) => (
                  <option key={status} value={status}>
                    {STATUS_LABELS[status]}
                  </option>
                ))}
              </select>
            </div>

            <div className="field">
              <label htmlFor="issue-priority">Priority</label>
              <select
                id="issue-priority"
                data-testid="issue-priority"
                value={detail.priority}
                onChange={(event) => props.onPriority(event.target.value as IssuePriority)}
              >
                {ISSUE_PRIORITIES.map((priority) => (
                  <option key={priority} value={priority}>
                    {PRIORITY_LABELS[priority]}
                  </option>
                ))}
              </select>
            </div>

            <div className="field">
              <label htmlFor="issue-assignee">Assignee</label>
              <select
                id="issue-assignee"
                data-testid="issue-assignee"
                value={detail.assigneeId ?? ""}
                onChange={(event) =>
                  props.onAssign(isKnownMember(event.target.value) ? event.target.value : null)
                }
              >
                <option value="">Unassigned</option>
                {TEAM.map((member) => (
                  <option key={member.id} value={member.id}>
                    {member.name}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <p className="meta">
            Created {relativeTime(detail.createdAt, props.now)} · updated{" "}
            {relativeTime(detail.updatedAt, props.now)}
          </p>

          <section className="comments" aria-label="Comments">
            <h3>Comments ({detail.comments.length})</h3>
            {detail.comments.length === 0 ? (
              <p className="empty">No comments yet.</p>
            ) : (
              <ul className="comment-list">
                {detail.comments.map((entry) => (
                  <li key={entry.commentId}>
                    <span className="comment-author">{memberName(entry.authorId)}</span>
                    <span className="meta">{relativeTime(entry.at, props.now)}</span>
                    <p>{entry.body}</p>
                  </li>
                ))}
              </ul>
            )}
            <form
              className="field"
              onSubmit={(event) => {
                event.preventDefault();
                if (comment.trim().length === 0) return;
                props.onComment(comment.trim());
                setComment("");
              }}
            >
              <label htmlFor="new-comment">Add a comment</label>
              <textarea
                id="new-comment"
                data-testid="new-comment"
                rows={2}
                value={comment}
                onChange={(event) => setComment(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
                    event.currentTarget.form?.requestSubmit();
                  }
                }}
              />
              <button
                type="submit"
                className="primary"
                data-testid="submit-comment"
                disabled={comment.trim().length === 0}
              >
                Comment
              </button>
            </form>
          </section>
        </div>
      )}
    </div>
  );
}
