import { Array, Option, Record } from "effect";
import type { Html, HtmlBuilder } from "foldkit/html";
import {
  issueStatuses,
  wipLimit,
  type IssueStatus,
} from "@streamsy/issue-tracker-demo-contract/state-schema";
import type { Comment, Issue, Project } from "@streamsy/issue-tracker-demo-contract/types";
import { STATUS_LABEL, formatRelativeTime, shortId } from "issue-tracker-demo/src/utils/format.ts";
import { titleDraftId } from "../command.ts";
import {
  commentsOn,
  issuesIn,
  projectsOldestFirst,
  selectedProject,
  statusCounts,
  visibleIssues,
  type Shown,
  type StatusFilter,
  type Workspace,
} from "../domain/workspace.ts";
import { Message } from "../message.ts";
import type { Model, TitleEdit } from "../model.ts";

const filters: ReadonlyArray<{ readonly value: StatusFilter; readonly label: string }> = [
  { value: "all", label: "All" },
  { value: "open", label: "Open" },
  { value: "in_progress", label: "In progress" },
  { value: "done", label: "Done" },
];

const statusOptions: Record<IssueStatus, string> = {
  open: "Open",
  in_progress: "In progress",
  done: "Done",
};

export const issuesPanelView = (
  model: Model,
  workspace: Workspace,
  h: HtmlBuilder<Message>,
): Html =>
  Option.match(selectedProject(projectsOldestFirst(workspace), model.maybeSelectedProjectId), {
    onNone: () =>
      h.section(
        [h.Class("panel issues")],
        [
          toolbarView("No project selected", "0 issues", h),
          h.div(
            [h.Class("issue-list")],
            [h.div([h.Class("empty-state")], ["Create a project to start tracking issues."])],
          ),
        ],
      ),
    onSome: ({ row: project }) => projectIssuesView(model, workspace, project, h),
  });

/** The derived values of A2: counts and the filtered list, computed on each render. */
const projectIssuesView = (
  model: Model,
  workspace: Workspace,
  project: Project,
  h: HtmlBuilder<Message>,
): Html => {
  const inProject = issuesIn(workspace, project.id);
  const counts = statusCounts(inProject);
  const visible = visibleIssues(inProject, model.filter);

  return h.section(
    [h.Class("panel issues")],
    [
      toolbarView(
        project.name,
        `${visible.length} ${visible.length === 1 ? "issue" : "issues"} · ${shortId(project.id)}`,
        h,
      ),
      newIssueFormView(model.issueTitle, h),
      filterRowView(model.filter, counts, h),
      h.div(
        [h.Class("issue-list")],
        Array.match(visible, {
          onEmpty: () => [emptyIssuesView(model.filter, h)],
          onNonEmpty: (issues) =>
            Array.map(issues, (issue) => issueRowView(model, workspace, issue, h)),
        }),
      ),
    ],
  );
};

const toolbarView = (title: string, summary: string, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class("issues-toolbar")],
    [h.div([h.Class("issues-title")], [h.h2([], [title]), h.p([], [summary])])],
  );

const newIssueFormView = (issueTitle: string, h: HtmlBuilder<Message>): Html =>
  h.form(
    [h.Class("new-issue-form"), h.OnSubmit(Message.SubmittedIssue())],
    [
      h.input([
        h.Value(issueTitle),
        h.OnInput((value) => Message.UpdatedIssueTitle({ value })),
        h.Placeholder("Describe a new issue…"),
        h.AriaLabel("New issue title"),
      ]),
      h.button([h.Type("submit")], ["Add issue"]),
    ],
  );

const filterRowView = (
  selected: StatusFilter,
  counts: Record<StatusFilter, number>,
  h: HtmlBuilder<Message>,
): Html =>
  h.div(
    [h.Class("filter-row"), h.Role("group"), h.AriaLabel("Filter issues by status")],
    Array.map(filters, ({ value, label }) =>
      h.keyed("button")(
        value,
        [
          h.Type("button"),
          h.Class(value === selected ? "filter-chip selected" : "filter-chip"),
          h.AriaPressed(String(value === selected)),
          h.OnClick(Message.SelectedFilter({ filter: value })),
        ],
        [
          label,
          h.span(
            [h.Class("filter-count")],
            [`${counts[value]}${value === "in_progress" ? `/${wipLimit}` : ""}`],
          ),
        ],
      ),
    ),
  );

const emptyIssuesView = (filter: StatusFilter, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class("empty-state")],
    [
      filter === "all" ? "No issues in this project yet." : "No issues with this status.",
      h.span(
        [h.Class("hint")],
        ["Create one above — mutations append events to the durable stream."],
      ),
    ],
  );

/** One issue. A pending row has a local change that the server has not confirmed, so it shows faded. */
const issueRowView = (
  model: Model,
  workspace: Workspace,
  { row: issue, isPending }: Shown<Issue>,
  h: HtmlBuilder<Message>,
): Html => {
  const comments = commentsOn(workspace, issue.id);
  const maybeEdit = Option.filter(model.maybeTitleEdit, (edit) => edit.issueId === issue.id);

  return h.keyed("article")(
    issue.id,
    [h.Class(isPending ? "issue-row pending" : "issue-row"), h.AriaBusy(isPending)],
    [
      h.span([h.Class(`status-pill status-${issue.status}`)], [STATUS_LABEL[issue.status]]),
      h.div(
        [h.Class("issue-main")],
        [
          Option.match(maybeEdit, {
            onNone: () => titleButtonView(issue, h),
            onSome: (edit) => titleInputView(issue, edit, h),
          }),
          h.div(
            [h.Class("issue-meta")],
            [
              h.span([h.Class("issue-id")], [`ISS-${shortId(issue.id)}`]),
              h.span([h.Class("issue-meta-divider")], ["·"]),
              h.span(
                [],
                [
                  `updated ${formatRelativeTime(issue.updatedAt)}${issue.updatedBy ? ` by ${issue.updatedBy}` : ""}`,
                ],
              ),
              h.span([h.Class("issue-meta-divider")], ["·"]),
              h.span([], [`${comments.length} ${comments.length === 1 ? "comment" : "comments"}`]),
            ],
          ),
        ],
      ),
      h.div([h.Class("issue-actions")], [statusSelectView(issue, h)]),
      commentsView(model, issue, comments, h),
    ],
  );
};

/** The title, as a button that turns into a text field. */
const titleButtonView = (issue: Issue, h: HtmlBuilder<Message>): Html =>
  h.h3(
    [h.Class("issue-title")],
    [
      h.button(
        [
          h.Type("button"),
          h.Class("issue-title-button"),
          h.Title("Rename"),
          h.OnClick(Message.ClickedIssueTitle({ issueId: issue.id })),
        ],
        [issue.title],
      ),
    ],
  );

/** Enter or blur saves; Escape cancels. */
const titleInputView = (issue: Issue, edit: TitleEdit, h: HtmlBuilder<Message>): Html =>
  h.input([
    h.Id(titleDraftId(issue.id)),
    h.Class("issue-title-input"),
    h.Value(edit.draft),
    h.AriaLabel(`New title for ${issue.title}`),
    h.OnInput((value) => Message.UpdatedTitleDraft({ value })),
    h.OnBlur(Message.BlurredTitleDraft()),
    h.OnKeyDownPreventDefault((key) =>
      key === "Enter" || key === "Escape"
        ? Option.some(Message.PressedTitleDraftKey({ key }))
        : Option.none(),
    ),
  ]);

const statusSelectView = (issue: Issue, h: HtmlBuilder<Message>): Html =>
  h.select(
    [
      h.AriaLabel(`Status for ${issue.title}`),
      h.Value(issue.status),
      h.OnChange((value) => Message.SelectedIssueStatus({ issueId: issue.id, value })),
    ],
    Array.map(issueStatuses, (status) =>
      h.option([h.Value(status), h.Selected(status === issue.status)], [statusOptions[status]]),
    ),
  );

const commentsView = (
  model: Model,
  issue: Issue,
  comments: ReadonlyArray<Shown<Comment>>,
  h: HtmlBuilder<Message>,
): Html =>
  h.section(
    [h.Class("comments")],
    [
      ...Array.match(comments, {
        onEmpty: () => [h.p([h.Class("comments-empty")], ["No comments yet."])],
        onNonEmpty: (shown) =>
          Array.map(shown, ({ row: comment, isPending }) =>
            h.keyed("p")(
              comment.id,
              [h.Class(isPending ? "comment pending" : "comment")],
              [h.span([h.Class("author")], [comment.author]), comment.body],
            ),
          ),
      }),
      h.form(
        [h.Class("comment-form"), h.OnSubmit(Message.SubmittedComment({ issueId: issue.id }))],
        [
          h.input([
            h.Value(Option.getOrElse(Record.get(model.commentDrafts, issue.id), () => "")),
            h.OnInput((value) => Message.UpdatedCommentDraft({ issueId: issue.id, value })),
            h.Placeholder("Add a comment"),
            h.AriaLabel(`Comment on ${issue.title}`),
          ]),
          h.button([h.Type("submit"), h.Class("subtle")], ["Comment"]),
        ],
      ),
    ],
  );
