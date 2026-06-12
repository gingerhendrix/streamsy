import { useState } from "react";
import type { Project } from "../../types.ts";

export function ProjectsPanel({
  projects,
  selectedProjectId,
  issueCountByProject,
  onSelect,
  onCreate,
}: {
  projects: Project[];
  selectedProjectId: string | undefined;
  issueCountByProject: Map<string, number>;
  onSelect: (projectId: string) => void;
  onCreate: (project: Project) => Promise<void>;
}) {
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");

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
          await onCreate(project);
          setProjectName("");
          setProjectDescription("");
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
  );
}
