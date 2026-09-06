import { useState, type FormEvent } from "react";
import type { Comment, Issue, IssueStatus, Project } from "../../shared/types.ts";
import { shortId } from "../utils/format.ts";
import { IssueRow } from "./IssueRow.tsx";

export function IssuesPanel({
  selectedProject,
  visibleIssues,
  commentsByIssue,
  onCreateIssue,
  onStatus,
  onComment,
}: {
  selectedProject: Project | undefined;
  visibleIssues: Issue[];
  commentsByIssue: Map<string, Comment[]>;
  onCreateIssue: (issue: Issue) => Promise<void>;
  onStatus: (issue: Issue, status: IssueStatus) => Promise<void>;
  onComment: (issue: Issue, body: string) => Promise<void>;
}) {
  const [issueTitle, setIssueTitle] = useState("");

  const submitNewIssue = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedProject || !issueTitle.trim()) return;
    const timestamp = new Date().toISOString();
    const issue: Issue = {
      id: `issue_${crypto.randomUUID().slice(0, 8)}`,
      projectId: selectedProject.id,
      title: issueTitle.trim(),
      status: "open",
      createdAt: timestamp,
      updatedAt: timestamp,
    };
    await onCreateIssue(issue);
    setIssueTitle("");
  };

  return (
    <section className="panel issues">
      <div className="issues-toolbar">
        <div className="issues-title">
          <h2>{selectedProject?.name ?? "No project selected"}</h2>
          <p>
            {visibleIssues.length} {visibleIssues.length === 1 ? "issue" : "issues"}
            {selectedProject ? ` · ${shortId(selectedProject.id)}` : ""}
          </p>
        </div>
      </div>

      {selectedProject ? (
        <form className="new-issue-form" onSubmit={submitNewIssue}>
          <input
            value={issueTitle}
            onChange={(event) => setIssueTitle(event.target.value)}
            placeholder="Describe a new issue…"
            aria-label="New issue title"
          />
          <button type="submit">Add issue</button>
        </form>
      ) : null}

      <div className="issue-list">
        {visibleIssues.length === 0 && selectedProject ? (
          <div className="empty-state">
            No issues in this project yet.
            <span className="hint">
              Create one above — mutations append events to the durable stream.
            </span>
          </div>
        ) : null}
        {!selectedProject ? (
          <div className="empty-state">Create a project to start tracking issues.</div>
        ) : null}
        {visibleIssues.map((issue) => (
          <IssueRow
            key={issue.id}
            issue={issue}
            comments={commentsByIssue.get(issue.id) ?? []}
            onStatus={(status) => void onStatus(issue, status)}
            onComment={(body) => void onComment(issue, body)}
          />
        ))}
      </div>
    </section>
  );
}
