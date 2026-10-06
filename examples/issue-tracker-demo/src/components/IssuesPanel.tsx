import { useState, type FormEvent } from "react";
import { wipLimit } from "@streamsy/issue-tracker-demo-contract/state-schema";
import type {
  Comment,
  Issue,
  IssueStatus,
  Project,
} from "@streamsy/issue-tracker-demo-contract/types";
import type { StatusFilter } from "../queries.ts";
import { shortId } from "../utils/format.ts";
import { IssueRow } from "./IssueRow.tsx";

type Synced<T> = T & { readonly $synced: boolean };

const filters: ReadonlyArray<{ value: StatusFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "open", label: "Open" },
  { value: "in_progress", label: "In progress" },
  { value: "done", label: "Done" },
];

export function IssuesPanel({
  selectedProject,
  visibleIssues,
  commentsByIssue,
  filter,
  statusCounts,
  onFilter,
  onCreateIssue,
  onStatus,
  onRename,
  onComment,
}: {
  selectedProject: Project | undefined;
  visibleIssues: ReadonlyArray<Synced<Issue>>;
  commentsByIssue: Map<string, ReadonlyArray<Synced<Comment>>>;
  filter: StatusFilter;
  statusCounts: Record<StatusFilter, number>;
  onFilter: (filter: StatusFilter) => void;
  onCreateIssue: (title: string) => Promise<void>;
  onStatus: (issue: Issue, status: IssueStatus) => void;
  onRename: (issue: Issue, title: string) => void;
  onComment: (issue: Issue, body: string) => Promise<void>;
}) {
  const [issueTitle, setIssueTitle] = useState("");

  // oxlint-disable-next-line effecttsgo/async-function -- React/browser callers await this Promise; preserve native rejection handling at this boundary.
  const submitNewIssue = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!selectedProject || !issueTitle.trim()) return;
    const title = issueTitle.trim();
    setIssueTitle("");
    await onCreateIssue(title);
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
        <>
          <form className="new-issue-form" onSubmit={submitNewIssue}>
            <input
              value={issueTitle}
              onChange={(event) => setIssueTitle(event.target.value)}
              placeholder="Describe a new issue…"
              aria-label="New issue title"
            />
            <button type="submit">Add issue</button>
          </form>

          <div className="filter-row" role="group" aria-label="Filter issues by status">
            {filters.map(({ value, label }) => (
              <button
                key={value}
                type="button"
                className={value === filter ? "filter-chip selected" : "filter-chip"}
                aria-pressed={value === filter}
                onClick={() => onFilter(value)}
              >
                {label}
                <span className="filter-count">
                  {statusCounts[value]}
                  {value === "in_progress" ? `/${wipLimit}` : ""}
                </span>
              </button>
            ))}
          </div>
        </>
      ) : null}

      <div className="issue-list">
        {visibleIssues.length === 0 && selectedProject ? (
          <div className="empty-state">
            {filter === "all" ? "No issues in this project yet." : "No issues with this status."}
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
            pending={!issue.$synced}
            comments={commentsByIssue.get(issue.id) ?? []}
            onStatus={(status) => onStatus(issue, status)}
            onRename={(title) => onRename(issue, title)}
            onComment={(body) => void onComment(issue, body)}
          />
        ))}
      </div>
    </section>
  );
}
