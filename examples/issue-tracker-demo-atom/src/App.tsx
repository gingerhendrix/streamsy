import { useAtomValue } from "@effect/atom-react";
import { Option } from "effect";
import { Topbar } from "./components/Topbar.tsx";
import { IssueTrackerApp } from "./components/IssueTrackerApp.tsx";
import { replicaAtom } from "./queries.ts";
import { workspaceIdAtom } from "./state.ts";

export function App() {
  const workspaceId = useAtomValue(workspaceIdAtom);
  const { isLive, maybeError } = useAtomValue(replicaAtom);

  if (isLive && Option.isNone(maybeError)) return <IssueTrackerApp workspaceId={workspaceId} />;

  return (
    <main className="shell">
      <Topbar workspaceId={workspaceId} />
      <div className="loading">
        {Option.getOrElse(maybeError, () => "Loading durable stream database…")}
      </div>
    </main>
  );
}
