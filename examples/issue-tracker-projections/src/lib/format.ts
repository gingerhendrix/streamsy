/** Display helpers. Every value here is derived from durable domain state. */
import type { IssuePriority, IssueStatus } from "../../shared/model.ts";
import { TEAM } from "../../shared/model.ts";

export const STATUS_LABELS: Readonly<Record<IssueStatus, string>> = {
  backlog: "Backlog",
  "in-progress": "In progress",
  done: "Done",
};

export const PRIORITY_LABELS: Readonly<Record<IssuePriority, string>> = {
  low: "Low",
  medium: "Medium",
  high: "High",
  urgent: "Urgent",
};

export function memberName(id: string | null): string {
  if (id === null) return "Unassigned";
  return TEAM.find((member) => member.id === id)?.name ?? id;
}

export function initials(id: string | null): string {
  if (id === null) return "–";
  const name = memberName(id);
  return name.slice(0, 2).toUpperCase();
}

/** Compact relative time. Fixed thresholds keep screenshots stable. */
export function relativeTime(iso: string, now: number = Date.now()): string {
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return "unknown";
  const seconds = Math.max(0, Math.round((now - at) / 1_000));
  if (seconds < 45) return "just now";
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 7) return `${days}d ago`;
  return new Date(at).toISOString().slice(0, 10);
}

/**
 * Trim durable stream offsets for dense inspector rows. Zero-padded composite
 * offsets (`<offset>_<sub>`) lose their padding, not their meaning.
 */
export function shortPosition(position: string | null): string {
  if (position === null) return "—";
  if (/^\d+(_\d+)*$/.test(position)) {
    return position
      .split("_")
      .map((part) => part.replace(/^0+(?=\d)/, ""))
      .join("_");
  }
  return position.length > 20 ? `…${position.slice(-18)}` : position;
}

export function shortStream(name: string): string {
  const parts = name.split("/");
  return parts.length <= 3 ? name : `…/${parts.slice(-3).join("/")}`;
}

/** Derive a stream-safe display key from a project name. */
export function projectKeyFrom(name: string): string {
  const letters = name.toUpperCase().replaceAll(/[^A-Z0-9]/g, "");
  return letters.length >= 3 ? letters.slice(0, 4) : `${letters}PRJ`.slice(0, 4);
}
