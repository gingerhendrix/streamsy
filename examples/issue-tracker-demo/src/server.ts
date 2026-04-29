import { HttpHandler, StreamProtocol } from "@streamsy/core";
import { createMemoryStorageFactory } from "@streamsy/storage-memory";
import type {
  BootstrapPayload,
  Comment,
  EntityByType,
  EntityType,
  Issue,
  IssueStatus,
  Project,
  StateEvent,
} from "./types.ts";

const port = Number.parseInt(process.env.PORT ?? "1338", 10);
const streamId = "session/main";
const encoder = new TextEncoder();

const storageFactory = createMemoryStorageFactory();
const protocol = new StreamProtocol(storageFactory);
const streamHandler = new HttpHandler({ protocol, pathPrefix: "/streams" });

const projects = new Map<string, Project>();
const issues = new Map<string, Issue>();
const comments = new Map<string, Comment>();

function json(data: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json",
      ...init.headers,
    },
  });
}

function now(): string {
  return new Date().toISOString();
}

function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().slice(0, 8)}`;
}

async function ensureStream(): Promise<void> {
  const result = await protocol.create(streamId, {
    contentType: "application/json",
  });
  if (result.status === "conflict" || result.status === "not-found" || result.status === "bad-request") {
    throw new Error(`Unable to create demo stream: ${result.status}`);
  }
}

async function appendEvent<T extends EntityType>(
  event: StateEvent<T>,
): Promise<string> {
  const result = await protocol.append(streamId, {
    data: encoder.encode(JSON.stringify(event)),
    contentType: "application/json",
  });
  if (result.status !== "appended" && result.status !== "duplicate") {
    throw new Error(`Unable to append state event: ${result.status}`);
  }
  return result.nextOffset;
}

async function emit<T extends EntityType>(
  type: T,
  operation: "insert" | "update" | "delete",
  key: string,
  value?: EntityByType[T],
  oldValue?: EntityByType[T],
): Promise<{ event: StateEvent<T>; offset: string }> {
  const event: StateEvent<T> = {
    type,
    key,
    value,
    old_value: oldValue,
    headers: {
      operation,
      timestamp: now(),
      txid: crypto.randomUUID(),
    },
  };
  const offset = await appendEvent(event);
  return { event, offset };
}

async function seed(): Promise<void> {
  if (projects.size > 0) return;
  const createdAt = now();
  const initialProjects: Project[] = [
    {
      id: "proj_streamsy",
      name: "Streamsy Demo",
      description: "A tiny issue tracker synced over durable streams.",
      createdAt,
    },
    {
      id: "proj_docs",
      name: "Writing",
      description: "Article examples and docs follow-ups.",
      createdAt,
    },
  ];

  const initialIssues: Issue[] = [
    {
      id: "issue_bootstrap",
      projectId: "proj_streamsy",
      title: "Bootstrap from snapshot, then long-poll the Streamsy log",
      status: "done",
      createdAt,
      updatedAt: createdAt,
    },
    {
      id: "issue_optimistic",
      projectId: "proj_streamsy",
      title: "Use TanStack DB for optimistic local writes",
      status: "in_progress",
      createdAt,
      updatedAt: createdAt,
    },
    {
      id: "issue_article",
      projectId: "proj_docs",
      title: "Connect the demo to the Part 1 sync-engine article",
      status: "open",
      createdAt,
      updatedAt: createdAt,
    },
  ];

  const initialComments: Comment[] = [
    {
      id: "comment_welcome",
      issueId: "issue_bootstrap",
      author: "demo-server",
      body: "The app starts with an HTTP snapshot and then consumes state events from /streams/session/main.",
      createdAt,
    },
  ];

  for (const project of initialProjects) {
    projects.set(project.id, project);
    await emit("project", "insert", project.id, project);
  }
  for (const issue of initialIssues) {
    issues.set(issue.id, issue);
    await emit("issue", "insert", issue.id, issue);
  }
  for (const comment of initialComments) {
    comments.set(comment.id, comment);
    await emit("comment", "insert", comment.id, comment);
  }
}

async function bootstrap(request: Request): Promise<Response> {
  const metadata = await protocol.metadata(streamId);
  const origin = new URL(request.url).origin;
  const payload: BootstrapPayload = {
    projects: [...projects.values()],
    issues: [...issues.values()],
    comments: [...comments.values()],
    streamUrl: `${origin}/streams/${streamId}`,
    nextOffset: metadata.nextOffset ?? "0_0",
  };
  return json(payload);
}

async function createProject(request: Request): Promise<Response> {
  const body = (await request.json()) as Partial<Project>;
  const project: Project = {
    id: body.id ?? id("proj"),
    name: String(body.name ?? "Untitled project").trim(),
    description: String(body.description ?? "").trim(),
    createdAt: now(),
  };
  projects.set(project.id, project);
  const result = await emit("project", "insert", project.id, project);
  return json({ project, awaitOffset: result.offset }, { status: 201 });
}

async function createIssue(request: Request): Promise<Response> {
  const body = (await request.json()) as Partial<Issue>;
  const projectId = String(body.projectId ?? "");
  if (!projects.has(projectId)) {
    return json({ error: "Unknown projectId" }, { status: 400 });
  }
  const timestamp = now();
  const issue: Issue = {
    id: body.id ?? id("issue"),
    projectId,
    title: String(body.title ?? "Untitled issue").trim(),
    status: body.status ?? "open",
    createdAt: timestamp,
    updatedAt: timestamp,
  };
  issues.set(issue.id, issue);
  const result = await emit("issue", "insert", issue.id, issue);
  return json({ issue, awaitOffset: result.offset }, { status: 201 });
}

async function updateIssue(request: Request, issueId: string): Promise<Response> {
  const previous = issues.get(issueId);
  if (!previous) return json({ error: "Issue not found" }, { status: 404 });

  const body = (await request.json()) as Partial<Issue>;
  const nextIssue: Issue = {
    ...previous,
    title: body.title === undefined ? previous.title : String(body.title).trim(),
    status: (body.status ?? previous.status) as IssueStatus,
    updatedAt: now(),
  };
  issues.set(issueId, nextIssue);
  const result = await emit("issue", "update", issueId, nextIssue, previous);
  return json({ issue: nextIssue, awaitOffset: result.offset });
}

async function createComment(request: Request): Promise<Response> {
  const body = (await request.json()) as Partial<Comment>;
  const issueId = String(body.issueId ?? "");
  if (!issues.has(issueId)) {
    return json({ error: "Unknown issueId" }, { status: 400 });
  }
  const comment: Comment = {
    id: body.id ?? id("comment"),
    issueId,
    author: String(body.author ?? "you").trim() || "you",
    body: String(body.body ?? "").trim(),
    createdAt: now(),
  };
  comments.set(comment.id, comment);
  const result = await emit("comment", "insert", comment.id, comment);
  return json({ comment, awaitOffset: result.offset }, { status: 201 });
}

async function routeApi(request: Request, url: URL): Promise<Response> {
  if (url.pathname === "/api/bootstrap" && request.method === "GET") {
    return bootstrap(request);
  }
  if (url.pathname === "/api/projects" && request.method === "POST") {
    return createProject(request);
  }
  if (url.pathname === "/api/issues" && request.method === "POST") {
    return createIssue(request);
  }
  const issueMatch = url.pathname.match(/^\/api\/issues\/([^/]+)$/);
  if (issueMatch && request.method === "PATCH") {
    return updateIssue(request, decodeURIComponent(issueMatch[1]!));
  }
  if (url.pathname === "/api/comments" && request.method === "POST") {
    return createComment(request);
  }
  return json({ error: "Not found" }, { status: 404 });
}

async function serveStatic(url: URL): Promise<Response> {
  const pathname = url.pathname === "/" ? "/index.html" : url.pathname;
  const candidate = `${import.meta.dir}/../dist${pathname}`;
  const file = Bun.file(candidate);
  if (await file.exists()) return new Response(file);

  if (!pathname.includes(".")) {
    const index = Bun.file(`${import.meta.dir}/../dist/index.html`);
    if (await index.exists()) return new Response(index);
  }

  return new Response(
    "Issue tracker API is running. Run `bun run build` in examples/issue-tracker-demo to serve the React app from this Bun server, or run `bun run dev:web` for Vite dev.",
    { status: 200, headers: { "content-type": "text/plain" } },
  );
}

await ensureStream();
await seed();

const server = Bun.serve({
  port,
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (url.pathname.startsWith("/streams/")) {
        return streamHandler.fetch(request);
      }
      if (url.pathname.startsWith("/api/")) {
        return routeApi(request, url);
      }
      return serveStatic(url);
    } catch (error) {
      console.error(error);
      return json({ error: "Internal server error" }, { status: 500 });
    }
  },
});

console.log(`Issue tracker demo listening on http://localhost:${server.port}`);
console.log(`Streamsy durable state stream: http://localhost:${server.port}/streams/${streamId}`);

export { server };
