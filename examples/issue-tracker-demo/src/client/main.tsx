import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { createStreamDB, type StreamDB } from "@durable-streams/state/db";
import { useLiveQuery } from "@tanstack/react-db";
import { issueTrackerState } from "../state-schema.ts";
import type { Comment, Issue, IssueStatus, Project } from "../types.ts";
import "./styles.css";

type OptimisticAction<T> = (variables: T) => { isPersisted: { promise: Promise<unknown> } };
type ApiMutationResult = { awaitOffset: string; txid: string };
type CreateProjectAction = { project: Project; txid: string };
type CreateIssueAction = { issue: Issue; txid: string };
type UpdateIssueStatusAction = {
  issue: Issue;
  status: IssueStatus;
  updatedAt: string;
  txid: string;
};
type CreateCommentAction = { comment: Comment; txid: string };
type IssueTrackerDb = StreamDB<typeof issueTrackerState> & {
  actions: {
    createProject: OptimisticAction<CreateProjectAction>;
    createIssue: OptimisticAction<CreateIssueAction>;
    updateIssueStatus: OptimisticAction<UpdateIssueStatusAction>;
    createComment: OptimisticAction<CreateCommentAction>;
  };
};

const STATUS_LABEL: Record<IssueStatus, string> = {
  open: "open",
  in_progress: "in progress",
  done: "done",
};

function streamUrl(): string {
  return new URL("/streams/session/main", window.location.origin).toString();
}

function createIssueTrackerDb(): IssueTrackerDb {
  return createStreamDB({
    streamOptions: {
      url: streamUrl(),
      contentType: "application/json",
      warnOnHttp: false,
    },
    state: issueTrackerState,
    actions: ({ db }) => ({
      createProject: {
        onMutate: ({ project }: CreateProjectAction) => {
          db.collections.projects.insert(project);
        },
        mutationFn: async ({ project, txid }: CreateProjectAction) => {
          const result = await postJson<ApiMutationResult>("/api/projects", { ...project, txid });
          await db.utils.awaitTxId(result.txid);
        },
      },
      createIssue: {
        onMutate: ({ issue }: CreateIssueAction) => {
          db.collections.issues.insert(issue);
        },
        mutationFn: async ({ issue, txid }: CreateIssueAction) => {
          const result = await postJson<ApiMutationResult>("/api/issues", { ...issue, txid });
          await db.utils.awaitTxId(result.txid);
        },
      },
      updateIssueStatus: {
        onMutate: ({ issue, status, updatedAt }: UpdateIssueStatusAction) => {
          db.collections.issues.update(issue.id, (draft) => {
            draft.status = status;
            draft.updatedAt = updatedAt;
          });
        },
        mutationFn: async ({ issue, status, updatedAt, txid }: UpdateIssueStatusAction) => {
          const result = await postJson<ApiMutationResult>(
            `/api/issues/${encodeURIComponent(issue.id)}`,
            { status, updatedAt, txid },
            { method: "PATCH" },
          );
          await db.utils.awaitTxId(result.txid);
        },
      },
      createComment: {
        onMutate: ({ comment }: CreateCommentAction) => {
          db.collections.comments.insert(comment);
        },
        mutationFn: async ({ comment, txid }: CreateCommentAction) => {
          const result = await postJson<ApiMutationResult>("/api/comments", { ...comment, txid });
          await db.utils.awaitTxId(result.txid);
        },
      },
    }),
  }) as IssueTrackerDb;
}

async function postJson<T>(url: string, body: unknown, init: RequestInit = {}): Promise<T> {
  const response = await fetch(url, {
    ...init,
    method: init.method ?? "POST",
    headers: { "content-type": "application/json", ...init.headers },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(await response.text());
  return (await response.json()) as T;
}

async function awaitOptimisticAction<T>(action: OptimisticAction<T>, variables: T): Promise<void> {
  const transaction = action(variables);
  await transaction.isPersisted.promise;
}

function formatRelativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  const diff = Date.now() - then;
  if (Number.isNaN(diff)) return "";
  const seconds = Math.round(diff / 1000);
  if (seconds < 60) return `${Math.max(seconds, 0)}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

function shortId(id: string): string {
  const idx = id.indexOf("_");
  return idx >= 0 ? id.slice(idx + 1).toUpperCase() : id.toUpperCase();
}

function IssueRow({
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

function App() {
  const [db, setDb] = useState<IssueTrackerDb | null>(null);

  useEffect(() => {
    let cancelled = false;
    const created = createIssueTrackerDb();

    void created.preload().then(() => {
      if (cancelled) {
        created.close();
        return;
      }
      setDb(created);
    });

    return () => {
      cancelled = true;
      created.close();
    };
  }, []);

  if (!db) {
    return (
      <main className="shell">
        <Topbar />
        <div className="loading">Loading durable stream database…</div>
      </main>
    );
  }

  return <IssueTrackerApp db={db} />;
}

function IssueTrackerApp({ db }: { db: IssueTrackerDb }) {
  const [selectedProjectId, setSelectedProjectId] = useState<string>("");
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");
  const [issueTitle, setIssueTitle] = useState("");

  const projectsQuery = useLiveQuery(db.collections.projects as any);
  const issuesQuery = useLiveQuery(db.collections.issues as any);
  const commentsQuery = useLiveQuery(db.collections.comments as any);

  const projects = (projectsQuery.data ?? []) as unknown as Project[];
  const issues = (issuesQuery.data ?? []) as unknown as Issue[];
  const comments = (commentsQuery.data ?? []) as unknown as Comment[];

  useEffect(() => {
    if (!selectedProjectId && projects[0]) setSelectedProjectId(projects[0].id);
  }, [projects, selectedProjectId]);

  const selectedProject =
    projects.find((project) => project.id === selectedProjectId) ?? projects[0];
  const visibleIssues = useMemo(
    () =>
      issues
        .filter((issue) => issue.projectId === selectedProject?.id)
        .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
    [issues, selectedProject?.id],
  );
  const issueCountByProject = useMemo(() => {
    const counts = new Map<string, number>();
    for (const issue of issues) counts.set(issue.projectId, (counts.get(issue.projectId) ?? 0) + 1);
    return counts;
  }, [issues]);
  const commentsByIssue = useMemo(() => {
    const grouped = new Map<string, Comment[]>();
    for (const comment of comments) {
      const list = grouped.get(comment.issueId) ?? [];
      list.push(comment);
      grouped.set(comment.issueId, list);
    }
    for (const list of grouped.values())
      list.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
    return grouped;
  }, [comments]);

  return (
    <main className="shell">
      <Topbar />

      <section className="layout">
        <aside className="panel projects">
          <div className="panel-header">
            <h2>Projects</h2>
            <span className="count-badge">{projects.length}</span>
          </div>
          <div className="project-list">
            {projects.length === 0 ? (
              <p className="project-empty">No projects yet</p>
            ) : (
              projects.map((project) => (
                <button
                  key={project.id}
                  type="button"
                  className={
                    project.id === selectedProject?.id
                      ? "project-button selected"
                      : "project-button"
                  }
                  onClick={() => setSelectedProjectId(project.id)}
                >
                  <span>
                    <span className="project-name">{project.name}</span>
                    {project.description ? (
                      <span className="project-description">{project.description}</span>
                    ) : null}
                  </span>
                  <span className="project-count">{issueCountByProject.get(project.id) ?? 0}</span>
                </button>
              ))
            )}
          </div>

          <form
            className="stack-form"
            onSubmit={async (event) => {
              event.preventDefault();
              if (!projectName.trim()) return;
              const project: Project = {
                id: `proj_${crypto.randomUUID().slice(0, 8)}`,
                name: projectName.trim(),
                description: projectDescription.trim(),
                createdAt: new Date().toISOString(),
              };
              await awaitOptimisticAction(db.actions.createProject, {
                project,
                txid: crypto.randomUUID(),
              });
              setProjectName("");
              setProjectDescription("");
              setSelectedProjectId(project.id);
            }}
          >
            <h3>New project</h3>
            <input
              value={projectName}
              onChange={(event) => setProjectName(event.target.value)}
              placeholder="Project name"
            />
            <textarea
              value={projectDescription}
              onChange={(event) => setProjectDescription(event.target.value)}
              placeholder="Description (optional)"
            />
            <button type="submit">Create project</button>
          </form>
        </aside>

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
            <form
              className="new-issue-form"
              onSubmit={async (event) => {
                event.preventDefault();
                if (!issueTitle.trim()) return;
                const timestamp = new Date().toISOString();
                const issue: Issue = {
                  id: `issue_${crypto.randomUUID().slice(0, 8)}`,
                  projectId: selectedProject.id,
                  title: issueTitle.trim(),
                  status: "open",
                  createdAt: timestamp,
                  updatedAt: timestamp,
                };
                await awaitOptimisticAction(db.actions.createIssue, {
                  issue,
                  txid: crypto.randomUUID(),
                });
                setIssueTitle("");
              }}
            >
              <input
                value={issueTitle}
                onChange={(event) => setIssueTitle(event.target.value)}
                placeholder="Describe a new issue…"
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
                onStatus={async (status) => {
                  await awaitOptimisticAction(db.actions.updateIssueStatus, {
                    issue,
                    status,
                    updatedAt: new Date().toISOString(),
                    txid: crypto.randomUUID(),
                  });
                }}
                onComment={async (body) => {
                  const comment: Comment = {
                    id: `comment_${crypto.randomUUID().slice(0, 8)}`,
                    issueId: issue.id,
                    author: "you",
                    body,
                    createdAt: new Date().toISOString(),
                  };
                  await awaitOptimisticAction(db.actions.createComment, {
                    comment,
                    txid: crypto.randomUUID(),
                  });
                }}
              />
            ))}
          </div>
        </section>
      </section>
    </main>
  );
}

function Topbar() {
  return (
    <header className="topbar">
      <div className="brand">
        <span className="brand-mark" aria-hidden="true">
          S
        </span>
        <span className="brand-name">Streamsy</span>
        <span className="brand-divider">/</span>
        <span className="brand-app">Issue Tracker</span>
      </div>
    </header>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
