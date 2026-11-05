import { useSyncExternalStore } from "react";
import type { Connection } from "../connection.ts";

/** Shows "Offline" while the stream reader cannot reach the server. */
export function ConnectionBadge({ connection }: { connection: Connection }) {
  const online = useSyncExternalStore(connection.subscribe, connection.isOnline);

  if (online) return null;

  return (
    <span className="offline-badge" title="Reconnecting to the workspace stream">
      Offline
    </span>
  );
}
