import { Array, Schema } from "effect";
import { useRef, useState } from "react";
import { IssueStatus } from "@streamsy/issue-tracker-demo-contract/state-schema";
import type { Comment, Issue } from "@streamsy/issue-tracker-demo-contract/types";
import { STATUS_LABEL, formatRelativeTime, shortId } from "../utils/format.ts";

/**
 * One issue. A `pending` row has a local change that the server has not
 * confirmed yet, so it shows faded.
 */
export function IssueRow({
  issue,
  pending,
  comments,
  onStatus,
  onRename,
  onComment,
}: {
  issue: Issue;
  pending: boolean;
  comments: ReadonlyArray<Comment & { readonly $synced: boolean }>;
  onStatus: (status: IssueStatus) => void;
  onRename: (title: string) => void;
  onComment: (body: string) => void;
}) {
  const [body, setBody] = useState("");

  return (
    <article className={pending ? "issue-row pending" : "issue-row"} aria-busy={pending}>
      <span className={`status-pill status-${issue.status}`}>{STATUS_LABEL[issue.status]}</span>
      <div className="issue-main">
        <IssueTitle title={issue.title} onRename={onRename} />
        <div className="issue-meta">
          <span className="issue-id">ISS-{shortId(issue.id)}</span>
          <span className="issue-meta-divider">·</span>
          <span>
            updated {formatRelativeTime(issue.updatedAt)}
            {issue.updatedBy ? ` by ${issue.updatedBy}` : ""}
          </span>
          <span className="issue-meta-divider">·</span>
          <span>
            {comments.length} {comments.length === 1 ? "comment" : "comments"}
          </span>
        </div>
      </div>
      <div className="issue-actions">
        <select
          aria-label={`Status for ${issue.title}`}
          value={issue.status}
          onChange={(event) => {
            // The select only offers the three statuses below; anything else
            // is not a status change.
            const status = event.target.value;

            if (Schema.is(IssueStatus)(status)) onStatus(status);
          }}
        >
          <option value="open">Open</option>
          <option value="in_progress">In progress</option>
          <option value="done">Done</option>
        </select>
      </div>
      <section className="comments">
        {comments.length === 0 ? (
          <p className="comments-empty">No comments yet.</p>
        ) : (
          Array.map(comments, (comment) => (
            <p key={comment.id} className={comment.$synced ? "comment" : "comment pending"}>
              <span className="author">{comment.author}</span>
              {comment.body}
            </p>
          ))
        )}
        <form
          className="comment-form"
          onSubmit={(event) => {
            event.preventDefault();

            if (!body.trim()) return;
            onComment(body.trim());
            setBody("");
          }}
        >
          <input
            value={body}
            onChange={(event) => setBody(event.target.value)}
            placeholder="Add a comment"
            aria-label={`Comment on ${issue.title}`}
          />
          <button type="submit" className="subtle">
            Comment
          </button>
        </form>
      </section>
    </article>
  );
}

/** The title, as a button that turns into a text field. Enter or blur saves; Escape cancels. */
function IssueTitle({ title, onRename }: { title: string; onRename: (title: string) => void }) {
  const [draft, setDraft] = useState<string | null>(null);
  // Closing the field removes it, and the browser can then send one more blur.
  // The ref makes sure that one edit saves at most once.
  const open = useRef(false);

  const close = () => {
    open.current = false;
    setDraft(null);
  };

  if (draft === null) {
    return (
      <h3 className="issue-title">
        <button
          type="button"
          className="issue-title-button"
          title="Rename"
          onClick={() => {
            open.current = true;
            setDraft(title);
          }}
        >
          {title}
        </button>
      </h3>
    );
  }

  const save = () => {
    if (!open.current) return;
    const next = draft.trim();
    close();

    if (next && next !== title) onRename(next);
  };

  return (
    <input
      className="issue-title-input"
      value={draft}
      aria-label={`New title for ${title}`}
      // The field appears because the user asked to rename.
      // oxlint-disable-next-line jsx-a11y/no-autofocus
      autoFocus
      onChange={(event) => setDraft(event.target.value)}
      onBlur={save}
      onKeyDown={(event) => {
        if (event.key === "Enter") save();

        if (event.key === "Escape") close();
      }}
    />
  );
}
