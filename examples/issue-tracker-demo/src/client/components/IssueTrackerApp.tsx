import { useEffect, useMemo, useState } from "react";
import { useLiveQuery } from "@tanstack/react-db";
import type { Comment, Issue, IssueStatus, Project } from "../../types.ts";
import { awaitOptimisticAction, type IssueDb } from "../db.ts";
import { Topbar } from "./Topbar.tsx";
import { ProjectsPanel } from "./ProjectsPanel.tsx";
import { IssuesPanel } from "./IssuesPanel.tsx";

export function IssueTrackerApp({ db }: { db: IssueDb }) {
  const [selectedProjectId, setSelectedProjectId] = useState<string>("");

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

  const createProject = async (project: Project) => {
    await awaitOptimisticAction(db.actions.createProject, {
      project,
      txid: crypto.randomUUID(),
    });
    setSelectedProjectId(project.id);
  };

  const createIssue = async (issue: Issue) => {
    await awaitOptimisticAction(db.actions.createIssue, {
      issue,
      txid: crypto.randomUUID(),
    });
  };

  const updateIssueStatus = async (issue: Issue, status: IssueStatus) => {
    await awaitOptimisticAction(db.actions.updateIssueStatus, {
      issue,
      status,
      updatedAt: new Date().toISOString(),
      txid: crypto.randomUUID(),
    });
  };

  const createComment = async (issue: Issue, body: string) => {
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
  };

  return (
    <main className="shell">
      <Topbar />

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
          onCreateIssue={createIssue}
          onStatus={updateIssueStatus}
          onComment={createComment}
        />
      </section>
    </main>
  );
}
