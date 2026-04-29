import React, { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { createDurableDb, type DurableDb } from "./durable-db.ts";
import type { Comment, Issue, IssueStatus, Project } from "../types.ts";
import "./styles.css";

function useDbSnapshot<T>(db: DurableDb | null, read: (db: DurableDb) => T, fallback: T): T {
  return useSyncExternalStore(
    (listener) => (db ? db.subscribe(listener) : () => undefined),
    () => (db ? read(db) : fallback),
    () => fallback,
  );
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

function IssueCard({
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
    <article className="issue-card">
      <div className="issue-title-row">
        <h3>{issue.title}</h3>
        <select value={issue.status} onChange={(event) => onStatus(event.target.value as IssueStatus)}>
          <option value="open">Open</option>
          <option value="in_progress">In progress</option>
          <option value="done">Done</option>
        </select>
      </div>
      <p className="muted">Updated {new Date(issue.updatedAt).toLocaleTimeString()}</p>
      <section className="comments">
        {comments.length === 0 ? <p className="muted">No comments yet.</p> : null}
        {comments.map((comment) => (
          <p key={comment.id} className="comment"><strong>{comment.author}:</strong> {comment.body}</p>
        ))}
      </section>
      <form
        className="inline-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (!body.trim()) return;
          onComment(body.trim());
          setBody("");
        }}
      >
        <input value={body} onChange={(event) => setBody(event.target.value)} placeholder="Add a comment" />
        <button type="submit">Comment</button>
      </form>
    </article>
  );
}

function App() {
  const [db, setDb] = useState<DurableDb | null>(null);
  const [selectedProjectId, setSelectedProjectId] = useState<string>("");
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");
  const [issueTitle, setIssueTitle] = useState("");
  const [lastOffset, setLastOffset] = useState("");

  useEffect(() => {
    let mounted = true;
    createDurableDb().then((created) => {
      if (!mounted) {
        created.stop();
        return;
      }
      setDb(created);
      setLastOffset(created.getOffset());
      void created.start();
    });
    return () => {
      mounted = false;
      db?.stop();
    };
  }, []);

  const projects = useDbSnapshot(db, (d) => d.projects.toArray as Project[], []);
  const issues = useDbSnapshot(db, (d) => d.issues.toArray as Issue[], []);
  const comments = useDbSnapshot(db, (d) => d.comments.toArray as Comment[], []);
  const offset = useDbSnapshot(db, (d) => d.getOffset(), lastOffset);

  useEffect(() => {
    if (!selectedProjectId && projects[0]) setSelectedProjectId(projects[0].id);
  }, [projects, selectedProjectId]);

  useEffect(() => setLastOffset(offset), [offset]);

  const selectedProject = projects.find((project) => project.id === selectedProjectId) ?? projects[0];
  const visibleIssues = useMemo(
    () => issues.filter((issue) => issue.projectId === selectedProject?.id).sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
    [issues, selectedProject?.id],
  );
  const commentsByIssue = useMemo(() => {
    const grouped = new Map<string, Comment[]>();
    for (const comment of comments) {
      const list = grouped.get(comment.issueId) ?? [];
      list.push(comment);
      grouped.set(comment.issueId, list);
    }
    for (const list of grouped.values()) list.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
    return grouped;
  }, [comments]);

  if (!db) return <main className="shell"><p>Loading durable DB snapshot...</p></main>;

  return (
    <main className="shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Streamsy + TanStack DB</p>
          <h1>Durable issue tracker</h1>
          <p>
            A tiny sync-engine demo: Bun mutates an in-memory model, emits State Protocol events into a
            Streamsy durable stream, and the React app materializes them into TanStack DB collections.
          </p>
        </div>
        <div className="offset-card">
          <span>current stream offset</span>
          <code>{offset}</code>
        </div>
      </header>

      <section className="layout">
        <aside className="panel projects">
          <h2>Projects</h2>
          {projects.map((project) => (
            <button
              key={project.id}
              className={project.id === selectedProject?.id ? "project-button selected" : "project-button"}
              onClick={() => setSelectedProjectId(project.id)}
            >
              <strong>{project.name}</strong>
              <span>{project.description}</span>
            </button>
          ))}

          <form
            className="stack-form"
            onSubmit={async (event) => {
              event.preventDefault();
              if (!projectName.trim()) return;
              const id = `proj_${crypto.randomUUID().slice(0, 8)}`;
              const createdAt = new Date().toISOString();
              db.projects.insert({ id, name: projectName, description: projectDescription, createdAt });
              const result = await postJson<{ awaitOffset: string }>("/api/projects", {
                id,
                name: projectName,
                description: projectDescription,
              });
              setLastOffset(result.awaitOffset);
              setProjectName("");
              setProjectDescription("");
              setSelectedProjectId(id);
            }}
          >
            <h3>New project</h3>
            <input value={projectName} onChange={(event) => setProjectName(event.target.value)} placeholder="Project name" />
            <textarea value={projectDescription} onChange={(event) => setProjectDescription(event.target.value)} placeholder="Description" />
            <button type="submit">Create project</button>
          </form>
        </aside>

        <section className="panel issues">
          <div className="section-title">
            <div>
              <h2>{selectedProject?.name ?? "No project selected"}</h2>
              <p>{visibleIssues.length} issues</p>
            </div>
          </div>

          {selectedProject ? (
            <form
              className="inline-form new-issue"
              onSubmit={async (event) => {
                event.preventDefault();
                if (!issueTitle.trim()) return;
                const timestamp = new Date().toISOString();
                const id = `issue_${crypto.randomUUID().slice(0, 8)}`;
                db.issues.insert({
                  id,
                  projectId: selectedProject.id,
                  title: issueTitle,
                  status: "open",
                  createdAt: timestamp,
                  updatedAt: timestamp,
                });
                const result = await postJson<{ awaitOffset: string }>("/api/issues", {
                  id,
                  projectId: selectedProject.id,
                  title: issueTitle,
                  status: "open",
                });
                setLastOffset(result.awaitOffset);
                setIssueTitle("");
              }}
            >
              <input value={issueTitle} onChange={(event) => setIssueTitle(event.target.value)} placeholder="New issue title" />
              <button type="submit">Add issue</button>
            </form>
          ) : null}

          <div className="issue-list">
            {visibleIssues.map((issue) => (
              <IssueCard
                key={issue.id}
                issue={issue}
                comments={commentsByIssue.get(issue.id) ?? []}
                onStatus={async (status) => {
                  const updatedAt = new Date().toISOString();
                  db.issues.update(issue.id, (draft) => {
                    draft.status = status;
                    draft.updatedAt = updatedAt;
                  });
                  const result = await postJson<{ awaitOffset: string }>(
                    `/api/issues/${encodeURIComponent(issue.id)}`,
                    { status },
                    { method: "PATCH" },
                  );
                  setLastOffset(result.awaitOffset);
                }}
                onComment={async (body) => {
                  const comment: Comment = {
                    id: `comment_${crypto.randomUUID().slice(0, 8)}`,
                    issueId: issue.id,
                    author: "you",
                    body,
                    createdAt: new Date().toISOString(),
                  };
                  db.comments.insert(comment);
                  const result = await postJson<{ awaitOffset: string }>("/api/comments", comment);
                  setLastOffset(result.awaitOffset);
                }}
              />
            ))}
          </div>
        </section>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
