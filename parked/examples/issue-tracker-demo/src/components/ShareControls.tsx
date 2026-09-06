import { useState } from "react";
import { createSharedWorkspace, gotoWorkspace, mainWorkspaceId } from "../utils/workspace.ts";

/**
 * Shareable-link controls: create a fresh shared workspace and copy the
 * current URL. Anyone who opens the copied link syncs to the same workspace
 * stream live.
 */
export function ShareControls({ workspaceId }: { workspaceId: string }) {
  const [creating, setCreating] = useState(false);
  const [copied, setCopied] = useState(false);

  const createWorkspace = async () => {
    setCreating(true);
    try {
      gotoWorkspace(await createSharedWorkspace());
    } catch (error) {
      console.error("Unable to create shared workspace", error);
      setCreating(false);
    }
  };

  const copyLink = async () => {
    await navigator.clipboard.writeText(window.location.href);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const isShared = workspaceId !== mainWorkspaceId;

  return (
    <div className="share-controls">
      {isShared && (
        <>
          <span className="share-workspace" title="Shared workspace id">
            {workspaceId}
          </span>
          <a className="share-back" href="?">
            Demo workspace
          </a>
        </>
      )}
      <button type="button" className="subtle" onClick={copyLink}>
        {copied ? "Copied" : "Copy link"}
      </button>
      <button type="button" onClick={createWorkspace} disabled={creating}>
        {creating ? "Creating…" : "New shared workspace"}
      </button>
    </div>
  );
}
