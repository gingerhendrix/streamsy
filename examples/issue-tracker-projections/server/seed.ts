/**
 * Deterministic, idempotent workspace seed.
 *
 * Every seeded command carries a fixed command id, so repeating the seed
 * reconciles through the producer tuple instead of duplicating issues.
 */
import { Effect } from "effect";
import type { IssuePriority, IssueStatus } from "../shared/domain.ts";
import type { MemberId } from "../shared/requests.ts";
import { createIssue, createProject, issueCommand, listProjects } from "./application.ts";

/**
 * Seed values are typed against the decoded request shapes rather than against
 * `string`, so a seed that the HTTP boundary would reject fails to compile
 * instead of failing at run time.
 */
interface SeedIssue {
  readonly issueId: string;
  readonly projectId: string;
  readonly title: string;
  readonly status: IssueStatus;
  readonly priority: IssuePriority;
  readonly assigneeId: MemberId | null;
  readonly comments: readonly {
    readonly commentId: string;
    readonly authorId: MemberId;
    readonly body: string;
  }[];
}

const PROJECTS = [
  { projectId: "launch", projectKey: "SHIP", name: "Launch" },
  { projectId: "platform", projectKey: "PLAT", name: "Platform" },
] as const;

const ISSUES: readonly SeedIssue[] = [
  {
    issueId: "issue-ship-1",
    projectId: "launch",
    title: "Ship the projection inspector",
    status: "in-progress",
    priority: "high",
    assigneeId: "ada",
    comments: [
      { commentId: "c-ship-1a", authorId: "grace", body: "Coverage labels read well now." },
    ],
  },
  {
    issueId: "issue-ship-2",
    projectId: "launch",
    title: "Write the board empty state",
    status: "backlog",
    priority: "low",
    assigneeId: null,
    comments: [],
  },
  {
    issueId: "issue-ship-3",
    projectId: "launch",
    title: "Prove chained coverage end to end",
    status: "done",
    priority: "urgent",
    assigneeId: "lin",
    comments: [
      { commentId: "c-ship-3a", authorId: "lin", body: "Two hops proven on the seeded workspace." },
      { commentId: "c-ship-3b", authorId: "omar", body: "Restart also recovers cleanly." },
    ],
  },
  {
    issueId: "issue-plat-1",
    projectId: "platform",
    title: "Bound the fan-in member scan",
    status: "in-progress",
    priority: "medium",
    assigneeId: "omar",
    comments: [],
  },
  {
    issueId: "issue-plat-2",
    projectId: "platform",
    title: "Document O(history) recovery",
    status: "backlog",
    priority: "medium",
    assigneeId: "grace",
    comments: [],
  },
  {
    issueId: "issue-plat-3",
    projectId: "platform",
    title: "Keep runtime keys out of deployment state",
    status: "done",
    priority: "high",
    assigneeId: "ada",
    comments: [
      { commentId: "c-plat-3a", authorId: "ada", body: "Alchemy state holds no workspace ids." },
    ],
  },
];

export const seedWorkspace = Effect.fn("Seed.workspace")(function* (workspaceId: string) {
  for (const project of PROJECTS) {
    yield* createProject(workspaceId, project);
  }
  for (const issue of ISSUES) {
    yield* createIssue(workspaceId, {
      commandId: `seed:create:${issue.issueId}`,
      issueId: issue.issueId,
      projectId: issue.projectId,
      title: issue.title,
      priority: issue.priority,
      creatorId: "ada",
    });
    if (issue.assigneeId !== null) {
      yield* issueCommand(workspaceId, issue.issueId, {
        commandId: `seed:assign:${issue.issueId}`,
        type: "assign",
        assigneeId: issue.assigneeId,
      });
    }
    if (issue.status !== "backlog") {
      yield* issueCommand(workspaceId, issue.issueId, {
        commandId: `seed:status:${issue.issueId}`,
        type: "status",
        status: issue.status,
      });
    }
    for (const comment of issue.comments) {
      yield* issueCommand(workspaceId, issue.issueId, {
        commandId: `seed:comment:${comment.commentId}`,
        type: "comment",
        commentId: comment.commentId,
        authorId: comment.authorId,
        body: comment.body,
      });
    }
  }
  const projects = yield* listProjects(workspaceId);
  return {
    workspaceId,
    projects: projects.map((project) => project.projectId),
    issues: ISSUES.map((issue) => issue.issueId),
  };
});

export const SEEDED_PROJECT_IDS = PROJECTS.map((project) => project.projectId);
export const SEEDED_ISSUE_IDS = ISSUES.map((issue) => issue.issueId);
