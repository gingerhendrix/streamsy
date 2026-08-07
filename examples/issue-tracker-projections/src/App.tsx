/**
 * Projection issue tracker workspace.
 *
 * Durable board and project rows arrive from their State streams. Commands go
 * to the API and return the exact accepted acknowledgement plus chained
 * coverage; the UI only reports `Synced` once the server proved the path.
 * Optimistic patches are display overlays that expire against durable rows.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { HealthResponse, IssueCommandRequest, MutationResponse } from "../shared/api.ts";
import type {
  BoardRow,
  IssueDetail,
  IssuePriority,
  IssueStatus,
  Project,
} from "../shared/model.ts";
import { BOARD_ROW_COLLECTION, PROJECT_COLLECTION, streamNames } from "../shared/model.ts";
import { Board } from "./components/Board.tsx";
import { Inspector } from "./components/Inspector.tsx";
import { IssueDrawer } from "./components/IssueDrawer.tsx";
import { api, ApiFailure, newId } from "./lib/api.ts";
import { memberName, PRIORITY_LABELS, projectKeyFrom, STATUS_LABELS } from "./lib/format.ts";
import { useNow, useStateFeed, useWorkspaceLocation } from "./lib/hooks.ts";
import {
  activeOverlays,
  cardSync,
  overlayRows,
  syncSummary,
  type Mutation,
  type PendingPatch,
} from "./lib/pending.ts";
import { sortBoardRows } from "./lib/state.ts";

const MUTATION_HISTORY = 12;

/** A detail load, tagged with the issue it belongs to. */
interface DetailResult {
  readonly issueId: string;
  readonly detail?: IssueDetail;
  readonly missing?: boolean;
}

interface CommandSpec {
  readonly commandId: string;
  readonly issueId: string;
  readonly projectId: string;
  readonly label: string;
  readonly patch: PendingPatch;
  readonly insert?: BoardRow;
  readonly send: () => Promise<MutationResponse>;
}

export function App() {
  const [location, navigate] = useWorkspaceLocation();
  const { workspaceId, projectId, issueId } = location;
  const now = useNow();

  const [health, setHealth] = useState<HealthResponse | undefined>(undefined);
  const [mutations, setMutations] = useState<readonly Mutation[]>([]);
  const [announcement, setAnnouncement] = useState("");
  const [banner, setBanner] = useState<string | undefined>(undefined);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [seeding, setSeeding] = useState(false);
  const [counts, setCounts] = useState<Readonly<Record<string, number>>>({});
  const [detailResult, setDetailResult] = useState<DetailResult | undefined>(undefined);
  const commands = useRef(new Map<string, CommandSpec>());

  // A result only counts for the issue it was loaded for, so switching issues
  // never shows the previous body or a stale "not found".
  const detail = detailResult?.issueId === issueId ? detailResult.detail : undefined;
  const detailMissing = detailResult?.issueId === issueId && detailResult.missing === true;

  const projectFeed = useStateFeed<Project>(streamNames.projects(workspaceId), PROJECT_COLLECTION);
  const projects = useMemo(
    () => Array.from(projectFeed.rows.values()).toSorted((a, b) => a.name.localeCompare(b.name)),
    [projectFeed.rows],
  );
  const activeProject =
    projects.find((project) => project.projectId === projectId) ?? projects[0] ?? undefined;
  const activeProjectId = activeProject?.projectId ?? null;

  const boardStream =
    activeProjectId === null ? null : streamNames.board(workspaceId, activeProjectId);
  const boardFeed = useStateFeed<BoardRow>(boardStream, BOARD_ROW_COLLECTION);

  useEffect(() => {
    api.health().then(setHealth, () => setHealth(undefined));
  }, []);

  // Keep the shareable URL in step with the resolved project.
  useEffect(() => {
    if (activeProjectId !== null && activeProjectId !== projectId) {
      navigate({ projectId: activeProjectId });
    }
  }, [activeProjectId, projectId, navigate]);

  const overlays = useMemo(
    () => activeOverlays(mutations, boardFeed.rows, now),
    [mutations, boardFeed.rows, now],
  );
  const rows = useMemo(
    () =>
      sortBoardRows(
        overlayRows(
          boardFeed.rows,
          overlays.filter((mutation) => mutation.projectId === activeProjectId),
        ),
      ),
    [boardFeed.rows, overlays, activeProjectId],
  );

  const summary = syncSummary(overlays);

  /** Run one command. The command id is stable, so a retry reconciles. */
  const run = useCallback(
    async (spec: CommandSpec) => {
      commands.current.set(spec.commandId, spec);
      setMutations((current) =>
        [
          {
            commandId: spec.commandId,
            issueId: spec.issueId,
            projectId: spec.projectId,
            label: spec.label,
            patch: spec.patch,
            ...(spec.insert === undefined ? {} : { insert: spec.insert }),
            phase: "syncing" as const,
            startedAt: Date.now(),
          },
          ...current.filter((mutation) => mutation.commandId !== spec.commandId),
        ].slice(0, MUTATION_HISTORY),
      );
      setBanner(undefined);

      try {
        const response = await spec.send();
        setMutations((current) =>
          current.map((mutation) =>
            mutation.commandId === spec.commandId
              ? {
                  ...mutation,
                  phase: "synced" as const,
                  settledAt: Date.now(),
                  ack: response.ack,
                  coverage: response.coverage,
                  error: undefined,
                }
              : mutation,
          ),
        );
        if (response.detail !== null && response.detail.issueId === issueId) {
          setDetailResult({ issueId, detail: response.detail });
        }
        if (response.coverage.status === "proven") {
          setAnnouncement(`${spec.label}: synced and proven through both projections.`);
        } else {
          setAnnouncement(`${spec.label}: accepted, projection catch-up in progress.`);
          // Convergence must not depend on this request; ask for a bounded repair.
          void api.repair(workspaceId, spec.projectId).catch(() => undefined);
        }
      } catch (error) {
        const message = error instanceof ApiFailure ? error.message : String(error);
        setMutations((current) =>
          current.map((mutation) =>
            mutation.commandId === spec.commandId
              ? { ...mutation, phase: "failed" as const, settledAt: Date.now(), error: message }
              : mutation,
          ),
        );
        setBanner(`${spec.label} failed: ${message}`);
        setAnnouncement(`${spec.label} failed: ${message}`);
      }
    },
    [issueId, workspaceId],
  );

  const retry = useCallback(
    (target: string) => {
      const spec = [...commands.current.values()].find(
        (candidate) => candidate.commandId === target || candidate.issueId === target,
      );
      if (spec !== undefined) void run(spec);
    },
    [run],
  );

  const changeStatus = useCallback(
    (row: BoardRow, status: IssueStatus) => {
      if (activeProjectId === null || row.status === status) return;
      const commandId = newId("cmd");
      void run({
        commandId,
        issueId: row.issueId,
        projectId: activeProjectId,
        label: `Move ${row.issueKey} to ${STATUS_LABELS[status]}`,
        patch: { status },
        send: () =>
          api.issueCommand(workspaceId, row.issueId, { commandId, type: "status", status }),
      });
    },
    [activeProjectId, run, workspaceId],
  );

  const createIssue = useCallback(
    (title: string, status: IssueStatus) => {
      if (activeProjectId === null) return;
      const commandId = newId("cmd");
      const targetIssueId = newId("issue");
      const at = new Date().toISOString();
      void run({
        commandId,
        issueId: targetIssueId,
        projectId: activeProjectId,
        label: `Create “${title}”`,
        patch: { title, status },
        insert: {
          issueId: targetIssueId,
          issueKey: "NEW",
          title,
          status,
          priority: "medium",
          assigneeId: null,
          commentCount: 0,
          updatedAt: at,
        },
        send: async () => {
          const created = await api.createIssue(workspaceId, {
            commandId,
            issueId: targetIssueId,
            projectId: activeProjectId,
            title,
            priority: "medium",
            status,
          });
          return created;
        },
      });
    },
    [activeProjectId, run, workspaceId],
  );

  const detailCommand = useCallback(
    (label: string, patch: PendingPatch, body: (commandId: string) => IssueCommandRequest) => {
      if (detail === undefined || activeProjectId === null) return;
      const commandId = newId("cmd");
      const request = body(commandId);
      void run({
        commandId,
        issueId: detail.issueId,
        projectId: activeProjectId,
        label,
        patch,
        send: () => api.issueCommand(workspaceId, detail.issueId, request),
      });
    },
    [activeProjectId, detail, run, workspaceId],
  );

  // Durable detail for the drawer, refreshed when its board row moves.
  const openRow = boardFeed.rows.get(issueId ?? "");
  const detailVersion = openRow?.updatedAt ?? "";
  useEffect(() => {
    if (issueId === null) {
      setDetailResult(undefined);
      return;
    }
    let cancelled = false;
    api.issueDetail(workspaceId, issueId).then(
      (loaded) => {
        if (!cancelled) setDetailResult({ issueId, detail: loaded });
      },
      () => {
        if (!cancelled) setDetailResult({ issueId, missing: true });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [workspaceId, issueId, detailVersion]);

  // Project rail counts come from the durable board of every project.
  const projectIds = projects.map((project) => project.projectId).join(",");
  useEffect(() => {
    if (projectIds.length === 0) return;
    let cancelled = false;
    const load = async () => {
      const entries = await Promise.all(
        projectIds.split(",").map(async (id) => {
          try {
            const board = await api.board(workspaceId, id);
            return [id, board.rows.length] as const;
          } catch {
            return [id, 0] as const;
          }
        }),
      );
      if (!cancelled) setCounts(Object.fromEntries(entries));
    };
    void load();
    const timer = setInterval(() => void load(), 10_000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [workspaceId, projectIds]);

  const closeDrawer = useCallback(() => {
    const previous = issueId;
    navigate({ issueId: null });
    // Return focus to the originating card once React has painted the board
    // again; the card may have moved column in the meantime.
    let attempts = 0;
    const restore = () => {
      const card = globalThis.document.querySelector<HTMLElement>(
        `[data-testid="card-${previous}"] .card-open`,
      );
      if (card !== null) {
        card.focus();
        return;
      }
      if (attempts++ < 12) globalThis.requestAnimationFrame(restore);
    };
    globalThis.requestAnimationFrame(restore);
  }, [issueId, navigate]);

  const seed = useCallback(async () => {
    setSeeding(true);
    try {
      await api.seed(workspaceId);
      setAnnouncement("Workspace seeded.");
    } catch (error) {
      setBanner(error instanceof ApiFailure ? error.message : String(error));
    } finally {
      setSeeding(false);
    }
  }, [workspaceId]);

  const needsSeed =
    projectFeed.status === "missing" || (projectFeed.ready && projects.length === 0);

  return (
    <div className={`app${issueId === null ? "" : " app-drawer"}`}>
      <header className="app-header">
        <span className="brand">Streamsy</span>
        <span className="workspace" data-testid="workspace-name">
          {workspaceId}
        </span>
        <WorkspaceSwitch
          workspaceId={workspaceId}
          onSwitch={(next) => navigate({ workspaceId: next, projectId: null, issueId: null })}
        />
        <button
          type="button"
          className="ghost"
          onClick={() => {
            void globalThis.navigator.clipboard
              ?.writeText(globalThis.location.href)
              .then(() => setAnnouncement("Workspace link copied."))
              .catch(() => setBanner("Clipboard is unavailable; copy the address bar instead."));
          }}
        >
          Copy link
        </button>

        <span className="spacer" />

        <span
          className={`state state-${boardFeed.status}`}
          data-testid="connection-state"
          title={health === undefined ? "" : `${health.host} · ${health.deployment}`}
        >
          {connectionLabel(boardFeed.status)}
        </span>
        <span className={`state sync-${summary.state}`} data-testid="sync-state">
          {syncLabel(summary)}
        </span>
        <button
          type="button"
          className="ghost"
          aria-expanded={inspectorOpen}
          data-testid="open-inspector"
          onClick={() => setInspectorOpen((open) => !open)}
        >
          Projections
        </button>
      </header>

      {banner !== undefined && (
        <p className="banner bad" role="alert" data-testid="error-banner">
          {banner}
          <button type="button" className="link" onClick={() => setBanner(undefined)}>
            Dismiss
          </button>
        </p>
      )}

      <div className="workspace-body">
        <nav className="rail" aria-label="Projects">
          <h2>Projects</h2>
          <ul>
            {projects.map((project) => (
              <li key={project.projectId}>
                <button
                  type="button"
                  className={`rail-item${
                    project.projectId === activeProjectId ? " rail-item-active" : ""
                  }`}
                  data-testid={`project-${project.projectId}`}
                  aria-current={project.projectId === activeProjectId}
                  onClick={() => navigate({ projectId: project.projectId, issueId: null })}
                >
                  <span className="rail-key">{project.projectKey}</span>
                  <span className="rail-name">{project.name}</span>
                  <span className="count">
                    {project.projectId === activeProjectId
                      ? boardFeed.rows.size
                      : (counts[project.projectId] ?? 0)}
                  </span>
                </button>
              </li>
            ))}
          </ul>
          <NewProject
            onCreate={async (name, key) => {
              try {
                const project = await api.createProject(workspaceId, {
                  projectId: newId("proj"),
                  projectKey: key,
                  name,
                });
                navigate({ projectId: project.projectId, issueId: null });
                setAnnouncement(`Project ${project.name} created.`);
              } catch (error) {
                setBanner(error instanceof ApiFailure ? error.message : String(error));
              }
            }}
          />
        </nav>

        <main className="main">
          {needsSeed ? (
            <section className="panel">
              <h1>Seed this workspace</h1>
              <p>
                Workspace <code>{workspaceId}</code> has no durable project stream yet. Seeding
                appends the demo projects and issues through the same command path the UI uses.
              </p>
              <button
                type="button"
                className="primary"
                onClick={() => void seed()}
                disabled={seeding}
                data-testid="seed-workspace"
              >
                {seeding ? "Seeding…" : "Seed demo workspace"}
              </button>
            </section>
          ) : (
            <Board
              rows={rows}
              loading={!boardFeed.ready && boardFeed.rows.size === 0}
              now={now}
              selectedIssueId={issueId}
              syncOf={(id) => cardSync(id, overlays)}
              errorOf={(id) =>
                overlays.find((mutation) => mutation.issueId === id && mutation.phase === "failed")
                  ?.error
              }
              onOpen={(id) => navigate({ issueId: id })}
              onStatusChange={changeStatus}
              onRetry={retry}
              onCreate={createIssue}
            />
          )}
        </main>

        {issueId !== null && (
          <IssueDrawer
            detail={detail}
            loading={!detailMissing}
            error={
              overlays.find(
                (mutation) => mutation.issueId === issueId && mutation.phase === "failed",
              )?.error
            }
            sync={cardSync(issueId, overlays)}
            now={now}
            onClose={closeDrawer}
            onRetry={() => retry(issueId)}
            onRename={(title) =>
              detailCommand(`Rename ${detail?.issueKey ?? "issue"}`, { title }, (commandId) => ({
                commandId,
                type: "rename",
                title,
              }))
            }
            onStatus={(status) =>
              detailCommand(
                `Move ${detail?.issueKey ?? "issue"} to ${STATUS_LABELS[status]}`,
                { status },
                (commandId) => ({ commandId, type: "status", status }),
              )
            }
            onPriority={(priority: IssuePriority) =>
              detailCommand(
                `Set ${detail?.issueKey ?? "issue"} priority to ${PRIORITY_LABELS[priority]}`,
                { priority },
                (commandId) => ({ commandId, type: "priority", priority }),
              )
            }
            onAssign={(assigneeId) =>
              detailCommand(
                `Assign ${detail?.issueKey ?? "issue"} to ${memberName(assigneeId)}`,
                { assigneeId },
                (commandId) => ({ commandId, type: "assign", assigneeId }),
              )
            }
            onComment={(body) =>
              detailCommand(
                `Comment on ${detail?.issueKey ?? "issue"}`,
                { commentCount: (detail?.comments.length ?? 0) + 1 },
                (commandId) => ({
                  commandId,
                  type: "comment",
                  commentId: newId("cmt"),
                  authorId: "ada",
                  body,
                }),
              )
            }
          />
        )}

        {inspectorOpen && (
          <Inspector
            mutations={mutations}
            boardStream={boardStream ?? "—"}
            onClose={() => setInspectorOpen(false)}
            onRepair={() => {
              if (activeProjectId === null) return;
              void api
                .repair(workspaceId, activeProjectId)
                .then(() => setAnnouncement("Repair pass completed."))
                .catch((error: unknown) => setBanner(String(error)));
            }}
          />
        )}
      </div>

      <p className="sr-only" role="status" aria-live="polite" data-testid="live-region">
        {announcement}
      </p>
    </div>
  );
}

function connectionLabel(status: string): string {
  if (status === "live") return "Live";
  if (status === "connecting") return "Connecting…";
  if (status === "missing") return "No board stream";
  return "Reconnecting…";
}

function syncLabel(summary: ReturnType<typeof syncSummary>): string {
  if (summary.state === "failed") return `${summary.count} failed`;
  if (summary.state === "syncing") return `Syncing ${summary.count}`;
  if (summary.state === "synced") return "Synced";
  return "Idle";
}

function WorkspaceSwitch(props: {
  readonly workspaceId: string;
  readonly onSwitch: (workspaceId: string) => void;
}) {
  const [value, setValue] = useState(props.workspaceId);
  useEffect(() => setValue(props.workspaceId), [props.workspaceId]);
  return (
    <form
      className="switch"
      onSubmit={(event) => {
        event.preventDefault();
        if (value.trim().length > 0) props.onSwitch(value.trim());
      }}
    >
      <label className="switch-label" htmlFor="workspace-input">
        Workspace
      </label>
      <input
        id="workspace-input"
        data-testid="workspace-input"
        value={value}
        onChange={(event) => setValue(event.target.value)}
      />
      <button type="submit" className="ghost">
        Open
      </button>
    </form>
  );
}

function NewProject(props: { readonly onCreate: (name: string, key: string) => void }) {
  const [name, setName] = useState("");
  return (
    <form
      className="new-project"
      onSubmit={(event) => {
        event.preventDefault();
        const trimmed = name.trim();
        if (trimmed.length === 0) return;
        props.onCreate(trimmed, projectKeyFrom(trimmed));
        setName("");
      }}
    >
      <label className="sr-only" htmlFor="new-project">
        New project name
      </label>
      <input
        id="new-project"
        data-testid="new-project"
        placeholder="New project"
        value={name}
        onChange={(event) => setName(event.target.value)}
      />
      <button type="submit" className="ghost" disabled={name.trim().length === 0}>
        Add
      </button>
    </form>
  );
}
