/** The always-present demo workspace, used when no `?w=` param is set. */
export const mainWorkspaceId = "main";

/** Workspace selected by the shareable link (`?w=<id>`). */
export function currentWorkspaceId(): string {
  const workspaceId = new URLSearchParams(window.location.search).get("w");
  return workspaceId?.trim() ? workspaceId.trim() : mainWorkspaceId;
}

/**
 * Switching workspace is a full navigation (reload), which keeps the stream
 * db lifecycle trivial: one workspace per page load.
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
export async function createSharedWorkspace(): Promise<string> {
  const response = await fetch("/api/workspaces", { method: "POST" });
  if (!response.ok) throw new Error(await response.text());
  const body = (await response.json()) as { id: string };
  return body.id;
}
