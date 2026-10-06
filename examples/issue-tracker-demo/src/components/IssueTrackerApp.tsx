import { useState } from "react";
import { anonymousAuthor } from "../../shared/state-schema.ts";
import type { Comment, Issue, IssueStatus, Project } from "../../shared/types.ts";
import { awaitOptimisticAction, type IssueDb, type OptimisticAction } from "../db.ts";
import {
  useCommentsByIssue,
  useIssueCountByProject,
  useProjects,
  useStatusCounts,
  useVisibleIssues,
  type StatusFilter,
} from "../queries.ts";
import { useDisplayName } from "../utils/display-name.ts";
import { useNotices } from "../utils/notices.ts";
import { ConnectionBadge } from "./ConnectionBadge.tsx";
import { IssuesPanel } from "./IssuesPanel.tsx";
import { NameField } from "./NameField.tsx";
import { Notices } from "./Notices.tsx";
import { ProjectsPanel } from "./ProjectsPanel.tsx";
import { Topbar } from "./Topbar.tsx";

const newId = (prefix: string) => `${prefix}_${crypto.randomUUID().slice(0, 8)}`;

export function IssueTrackerApp({ db, workspaceId }: { db: IssueDb; workspaceId: string }) {
  // Holds an explicit user selection only; while empty, rendering falls back to
  // the first project below — no effect needed to "initialize" the selection.
  const [selectedProjectId, setSelectedProjectId] = useState<string>("");
  const [filter, setFilter] = useState<StatusFilter>("all");
  const [name, setName] = useDisplayName();
  const { notices, notify } = useNotices();
  const writer = name.trim() || undefined;

  const projects = useProjects(db);
  const selectedProject =
    projects.find((project) => project.id === selectedProjectId) ?? projects[0];
  const projectId = selectedProject?.id ?? "";
  const issueCountByProject = useIssueCountByProject(db);
  const visibleIssues = useVisibleIssues(db, projectId, filter);
  const statusCounts = useStatusCounts(db, projectId);
  const commentsByIssue = useCommentsByIssue(db);

  /**
   * Run one optimistic action. If the server refuses it, TanStack DB rolls the
   * local change back, and the page says why.
   */
  const run = async <T,>(action: OptimisticAction<T>, variables: T) => {
    try {
      await awaitOptimisticAction(action, variables);
      return true;
    } catch (error) {
      notify(error instanceof Error ? error.message : String(error));
      return false;
    }
  };

  const createProject = async (projectName: string, description: string) => {
    const project: Project = {
      id: newId("proj"),
      name: projectName,
      description,
      createdAt: new Date().toISOString(),
    };
    const txid = crypto.randomUUID();
    if (await run(db.actions.createProject, { project, txid })) setSelectedProjectId(project.id);
  };

  const createIssue = async (title: string) => {
    if (!selectedProject) return;
    const now = new Date().toISOString();
    const unnamed: Issue = {
      id: newId("issue"),
      projectId: selectedProject.id,
      title,
      status: "open",
      createdAt: now,
      updatedAt: now,
    };
    const issue = writer === undefined ? unnamed : { ...unnamed, updatedBy: writer };
    await run(db.actions.createIssue, { issue, txid: crypto.randomUUID() });
  };

  const updateIssue = (issue: Issue, patch: { title?: string; status?: IssueStatus }) =>
    run(db.actions.updateIssue, { issue, patch, updatedBy: writer, txid: crypto.randomUUID() });

  const createComment = async (issue: Issue, body: string) => {
    const comment: Comment = {
      id: newId("comment"),
      issueId: issue.id,
      author: writer ?? anonymousAuthor,
      body,
      createdAt: new Date().toISOString(),
    };
    await run(db.actions.createComment, { comment, txid: crypto.randomUUID() });
  };

  return (
    <main className="shell">
      <Topbar workspaceId={workspaceId}>
        <ConnectionBadge connection={db.connection} />
        <NameField name={name} onName={setName} />
      </Topbar>

      <section className="layout">
        <ProjectsPanel
          projects={projects}
          selectedProjectId={selectedProject?.id}
          issueCountByProject={issueCountByProject}
          onSelect={setSelectedProjectId}
          onCreate={createProject}
        />

        <IssuesPanel
          selectedProject={selectedProject}
          visibleIssues={visibleIssues}
          commentsByIssue={commentsByIssue}
          filter={filter}
          statusCounts={statusCounts}
          onFilter={setFilter}
          onCreateIssue={createIssue}
          onStatus={(issue, status) => void updateIssue(issue, { status })}
          onRename={(issue, title) => void updateIssue(issue, { title })}
          onComment={createComment}
        />
      </section>

      <Notices notices={notices} />
    </main>
  );
}
