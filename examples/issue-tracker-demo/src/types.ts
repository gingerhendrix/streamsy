export type Project = {
  id: string;
  name: string;
  description: string;
  createdAt: string;
};

export type IssueStatus = "open" | "in_progress" | "done";

export type Issue = {
  id: string;
  projectId: string;
  title: string;
  status: IssueStatus;
  createdAt: string;
  updatedAt: string;
};

export type Comment = {
  id: string;
  issueId: string;
  author: string;
  body: string;
  createdAt: string;
};

export type EntityType = "project" | "issue" | "comment";

export type EntityByType = {
  project: Project;
  issue: Issue;
  comment: Comment;
};

export type StateEvent<T extends EntityType = EntityType> = {
  type: T;
  key: string;
  value?: EntityByType[T];
  old_value?: EntityByType[T];
  headers: {
    operation: "insert" | "update" | "delete";
    timestamp: string;
    txid: string;
  };
};

export type BootstrapPayload = {
  projects: Project[];
  issues: Issue[];
  comments: Comment[];
  streamUrl: string;
  nextOffset: string;
};
