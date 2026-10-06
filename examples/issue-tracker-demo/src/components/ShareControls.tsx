import { Effect } from "effect";
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

  // oxlint-disable-next-line effecttsgo/async-function -- React/browser callers await this Promise; preserve native rejection handling at this boundary.
  const createWorkspace = async () => {
    setCreating(true);

    try {
      gotoWorkspace(await createSharedWorkspace());
    } catch (error) {
      Effect.runSync(Effect.logError("Unable to create shared workspace", error));
      setCreating(false);
    }
  };

  // oxlint-disable-next-line effecttsgo/async-function -- React/browser callers await this Promise; preserve native rejection handling at this boundary.
  const copyLink = async () => {
    await navigator.clipboard.writeText(window.location.href);
    setCopied(true);
    Effect.runFork(Effect.sleep(1500).pipe(Effect.andThen(Effect.sync(() => setCopied(false)))));
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
