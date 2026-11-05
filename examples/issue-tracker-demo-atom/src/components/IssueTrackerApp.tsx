import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option } from "effect";
import type { Issue, IssueStatus } from "@streamsy/issue-tracker-demo-contract/types";
import { Intent } from "../domain/intent.ts";
import {
  commentsByIssueAtom,
  issueCountByProjectAtom,
  projectsAtom,
  selectedProjectAtom,
  statusCountsAtom,
  visibleIssuesAtom,
} from "../queries.ts";
import { filterAtom, selectedProjectIdAtom } from "../state.ts";
import { connection } from "../stream.ts";
import { displayNameAtom } from "../utils/display-name.ts";
import { noticesAtom } from "../utils/notices.ts";
import { writeAtom } from "../writes.ts";
import { ConnectionBadge } from "./ConnectionBadge.tsx";
import { IssuesPanel } from "./IssuesPanel.tsx";
import { NameField } from "./NameField.tsx";
import { Notices } from "./Notices.tsx";
import { ProjectsPanel } from "./ProjectsPanel.tsx";
import { Topbar } from "./Topbar.tsx";

export function IssueTrackerApp({ workspaceId }: { workspaceId: string }) {
  const [name, setName] = useAtom(displayNameAtom);
  const [filter, setFilter] = useAtom(filterAtom);
  const selectProject = useAtomSet(selectedProjectIdAtom);
  const write = useAtomSet(writeAtom);
  const notices = useAtomValue(noticesAtom);
  const projects = useAtomValue(projectsAtom);
  const selectedProject = Option.getOrUndefined(useAtomValue(selectedProjectAtom));
  const issueCountByProject = useAtomValue(issueCountByProjectAtom);
  const visibleIssues = useAtomValue(visibleIssuesAtom);
  const statusCounts = useAtomValue(statusCountsAtom);
  const commentsByIssue = useAtomValue(commentsByIssueAtom);

  /** The panels clear their fields when the Promise resolves. The write runs on in `writeAtom`. */
  const submit = (intent: Intent) => {
    write(intent);

    return Promise.resolve();
  };

  const updateIssue = (issue: Issue, patch: { title?: string; status?: IssueStatus }) => {
    write(Intent.UpdateIssue({ issueId: issue.id, patch }));
  };

  return (
    <main className="shell">
      <Topbar workspaceId={workspaceId}>
        <ConnectionBadge connection={connection} />
        <NameField name={name} onName={setName} />
      </Topbar>

      <section className="layout">
        <ProjectsPanel
          projects={projects}
          selectedProjectId={selectedProject?.id}
          issueCountByProject={issueCountByProject}
          onSelect={(projectId) => selectProject(Option.some(projectId))}
          onCreate={(projectName, description) =>
            submit(Intent.CreateProject({ name: projectName, description }))
          }
        />

        <IssuesPanel
          selectedProject={selectedProject}
          visibleIssues={visibleIssues}
          commentsByIssue={commentsByIssue}
          filter={filter}
          statusCounts={statusCounts}
          onFilter={setFilter}
          onCreateIssue={(title) =>
            selectedProject
              ? submit(Intent.CreateIssue({ projectId: selectedProject.id, title }))
              : Promise.resolve()
          }
          onStatus={(issue, status) => updateIssue(issue, { status })}
          onRename={(issue, title) => updateIssue(issue, { title })}
          onComment={(issue, body) => submit(Intent.CreateComment({ issueId: issue.id, body }))}
        />
      </section>

      <Notices notices={notices} />
    </main>
  );
}
