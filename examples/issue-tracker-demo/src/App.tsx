import { useEffect, useState } from "react";
import { createIssueDb, type IssueDb } from "./db.ts";
import { Topbar } from "./components/Topbar.tsx";
import { IssueTrackerApp } from "./components/IssueTrackerApp.tsx";
import { currentWorkspaceId } from "./utils/workspace.ts";

export function App() {
  const [db, setDb] = useState<IssueDb | null>(null);
  // Read once per page load: switching workspace is a full navigation, so the
  // db lifecycle is keyed on a stable workspace id.
  const workspaceId = currentWorkspaceId();

  useEffect(() => {
    let cancelled = false;
    const created = createIssueDb(workspaceId);

    void created.preload().then(() => {
      if (cancelled) {
        created.close();
        return;
      }
      setDb(created);
    });

    return () => {
      cancelled = true;
      created.close();
    };
  }, [workspaceId]);

  if (!db) {
    return (
      <main className="shell">
        <Topbar workspaceId={workspaceId} />
        <div className="loading">Loading durable stream database…</div>
      </main>
    );
  }

  return <IssueTrackerApp db={db} workspaceId={workspaceId} />;
}
