import type { IssueStatus } from "../types.ts";

export const STATUS_LABEL: Record<IssueStatus, string> = {
  open: "open",
  in_progress: "in progress",
  done: "done",
};

export function formatRelativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  const diff = Date.now() - then;
  if (Number.isNaN(diff)) return "";
  const seconds = Math.round(diff / 1000);
  if (seconds < 60) return `${Math.max(seconds, 0)}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

export function shortId(id: string): string {
  const idx = id.indexOf("_");
  return idx >= 0 ? id.slice(idx + 1).toUpperCase() : id.toUpperCase();
}
