import type { ReactNode } from "react";
import { ShareControls } from "./ShareControls.tsx";

/** The page header. `children` sit between the brand and the share controls. */
export function Topbar({ workspaceId, children }: { workspaceId: string; children?: ReactNode }) {
  return (
    <header className="topbar">
      <div className="brand">
        <span className="brand-mark" aria-hidden="true">
          S
        </span>
        <span className="brand-name">Streamsy</span>
        <span className="brand-divider">/</span>
        <span className="brand-app">Issue Tracker</span>
      </div>
      <div className="topbar-tools">
        {children}
        <ShareControls workspaceId={workspaceId} />
      </div>
    </header>
  );
}
