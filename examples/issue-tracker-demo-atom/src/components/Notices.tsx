import { Array } from "effect";
import type { Notice } from "../utils/notices.ts";

export function Notices({ notices }: { notices: ReadonlyArray<Notice> }) {
  return (
    <div className="notices" role="status" aria-live="polite">
      {Array.map(notices, (notice) => (
        <p key={notice.id} className="notice">
          <span className="notice-label">Change rejected</span>
          {notice.message}
        </p>
      ))}
    </div>
  );
}
