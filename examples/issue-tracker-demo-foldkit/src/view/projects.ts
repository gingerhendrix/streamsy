import { Array, Option } from "effect";
import type { Html, HtmlBuilder } from "foldkit/html";
import type { Project } from "@streamsy/issue-tracker-demo-contract/types";
import {
  issueCountByProject,
  projectsOldestFirst,
  selectedProject,
  type Shown,
  type Workspace,
} from "../domain/workspace.ts";
import { Message } from "../message.ts";
import type { Model } from "../model.ts";

export const projectsPanelView = (
  model: Model,
  workspace: Workspace,
  h: HtmlBuilder<Message>,
): Html => {
  const projects = projectsOldestFirst(workspace);

  const selectedId = Option.map(
    selectedProject(projects, model.maybeSelectedProjectId),
    ({ row }) => row.id,
  );

  const counts = issueCountByProject(workspace);

  return h.aside(
    [h.Class("panel projects")],
    [
      h.div(
        [h.Class("panel-header")],
        [h.h2([], ["Projects"]), h.span([h.Class("count-badge")], [String(projects.length)])],
      ),
      h.div(
        [h.Class("project-list")],
        Array.match(projects, {
          onEmpty: () => [h.p([h.Class("project-empty")], ["No projects yet"])],
          onNonEmpty: (shown) =>
            Array.map(shown, (project) =>
              projectButtonView(
                project,
                Option.contains(selectedId, project.row.id),
                counts[project.row.id] ?? 0,
                h,
              ),
            ),
        }),
      ),
      newProjectFormView(model, h),
    ],
  );
};

const projectButtonView = (
  { row: project }: Shown<Project>,
  isSelected: boolean,
  issueCount: number,
  h: HtmlBuilder<Message>,
): Html =>
  h.keyed("button")(
    project.id,
    [
      h.Type("button"),
      h.Class(isSelected ? "project-button selected" : "project-button"),
      h.OnClick(Message.SelectedProject({ projectId: project.id })),
    ],
    [
      h.span(
        [],
        [
          h.span([h.Class("project-name")], [project.name]),
          project.description === ""
            ? h.empty
            : h.span([h.Class("project-description")], [project.description]),
        ],
      ),
      h.span([h.Class("project-count")], [String(issueCount)]),
    ],
  );

const newProjectFormView = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.form(
    [h.Class("stack-form"), h.OnSubmit(Message.SubmittedProject())],
    [
      h.h3([], ["New project"]),
      h.input([
        h.Value(model.projectName),
        h.OnInput((value) => Message.UpdatedProjectName({ value })),
        h.Placeholder("Project name"),
        h.AriaLabel("Project name"),
      ]),
      h.textarea([
        h.Value(model.projectDescription),
        h.OnInput((value) => Message.UpdatedProjectDescription({ value })),
        h.Placeholder("Description (optional)"),
        h.AriaLabel("Project description"),
      ]),
      h.button([h.Type("submit")], ["Create project"]),
    ],
  );
