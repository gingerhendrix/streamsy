import { Schema } from "effect";
import { WorkspaceCreated } from "@streamsy/issue-tracker-demo-contract/api-schema";

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
export function createSharedWorkspace(): Promise<string> {
  // oxlint-disable-next-line effecttsgo/global-fetch -- This browser navigation boundary consumes a native Response before reloading the selected workspace.
  return fetch("/api/workspaces", { method: "POST" }).then((response) => {
    if (!response.ok) return response.text().then((text) => Promise.reject(new Error(text)));

    return response.json().then((body) => Schema.decodeUnknownSync(WorkspaceCreated)(body).id);
  });
}
