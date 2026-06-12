import { useEffect, useState } from "react";
import { createIssueDb, type IssueDb } from "../db.ts";
import { Topbar } from "./Topbar.tsx";
import { IssueTrackerApp } from "./IssueTrackerApp.tsx";

export function App() {
  const [db, setDb] = useState<IssueDb | null>(null);

  useEffect(() => {
    let cancelled = false;
    const created = createIssueDb();

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
  }, []);

  if (!db) {
    return (
      <main className="shell">
        <Topbar />
        <div className="loading">Loading durable stream database…</div>
      </main>
    );
  }

  return <IssueTrackerApp db={db} />;
}
