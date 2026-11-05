import { Effect } from "effect";
import { IssueTrackerClient, liveClient } from "../runtime.ts";

/** The always-present demo workspace, used when no `?w=` param is set. */
export const mainWorkspaceId = "main";

/** Workspace selected by the shareable link (`?w=<id>`). */
export function currentWorkspaceId(): string {
  const workspaceId = new URLSearchParams(window.location.search).get("w");

  return workspaceId?.trim() ? workspaceId.trim() : mainWorkspaceId;
}

/**
 * Switching workspace is a full navigation (reload), which keeps the stream
 * read trivial: one workspace per page load.
 */
export function gotoWorkspace(workspaceId: string): void {
  const url = new URL(window.location.href);

  if (workspaceId === mainWorkspaceId) {
    url.searchParams.delete("w");
  } else {
    url.searchParams.set("w", workspaceId);
  }

  window.location.assign(url.toString());
}

/** Create a fresh shared workspace on the server and return its id. */
export function createSharedWorkspace(): Promise<string> {
  return Effect.runPromise(
    IssueTrackerClient.use((client) => client.workspaces.create()).pipe(
      Effect.map(({ id }) => id),
      // This promise is its own entry point: the page navigates away when it resolves.
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      Effect.provide(liveClient),
    ),
  );
}
