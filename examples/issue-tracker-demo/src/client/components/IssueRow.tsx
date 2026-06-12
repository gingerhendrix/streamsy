import { useState } from "react";
import type { Comment, Issue, IssueStatus } from "../../types.ts";
import { STATUS_LABEL, formatRelativeTime, shortId } from "../format.ts";

export function IssueRow({
  issue,
  comments,
  onStatus,
  onComment,
}: {
  issue: Issue;
  comments: Comment[];
  onStatus: (status: IssueStatus) => void;
  onComment: (body: string) => void;
}) {
  const [body, setBody] = useState("");
  return (
    <article className="issue-row">
      <span className={`status-pill status-${issue.status}`}>{STATUS_LABEL[issue.status]}</span>
      <div className="issue-main">
        <h3 className="issue-title">{issue.title}</h3>
        <div className="issue-meta">
          <span className="issue-id">ISS-{shortId(issue.id)}</span>
          <span className="issue-meta-divider">·</span>
          <span>updated {formatRelativeTime(issue.updatedAt)}</span>
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
          onChange={(event) => onStatus(event.target.value as IssueStatus)}
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
          comments.map((comment) => (
            <p key={comment.id} className="comment">
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
