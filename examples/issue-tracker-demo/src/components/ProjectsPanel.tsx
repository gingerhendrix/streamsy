import { useState, type FormEvent } from "react";
import type { Project } from "../../shared/types.ts";

export function ProjectsPanel({
  projects,
  selectedProjectId,
  issueCountByProject,
  onSelect,
  onCreate,
}: {
  projects: ReadonlyArray<Project>;
  selectedProjectId: string | undefined;
  issueCountByProject: Map<string, number>;
  onSelect: (projectId: string) => void;
  onCreate: (name: string, description: string) => Promise<void>;
}) {
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");

  // oxlint-disable-next-line effecttsgo/async-function -- React/browser callers await this Promise; preserve native rejection handling at this boundary.
  const submitNewProject = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!projectName.trim()) return;
    await onCreate(projectName.trim(), projectDescription.trim());
    setProjectName("");
    setProjectDescription("");
  };

  return (
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
                project.id === selectedProjectId ? "project-button selected" : "project-button"
              }
              onClick={() => onSelect(project.id)}
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

      <form className="stack-form" onSubmit={submitNewProject}>
        <h3>New project</h3>
        <input
          value={projectName}
          onChange={(event) => setProjectName(event.target.value)}
          placeholder="Project name"
          aria-label="Project name"
        />
        <textarea
          value={projectDescription}
          onChange={(event) => setProjectDescription(event.target.value)}
          placeholder="Description (optional)"
          aria-label="Project description"
        />
        <button type="submit">Create project</button>
      </form>
    </aside>
  );
}
