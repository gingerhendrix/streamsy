import { useState } from "react";

const storageKey = "issue-tracker-demo:display-name";

/**
 * The name this browser writes with, kept in `localStorage`. Each tab of the
 * same browser starts with the last saved name.
 */
export function useDisplayName(): [string, (name: string) => void] {
  const [name, setName] = useState(() => localStorage.getItem(storageKey) ?? "");
  const update = (next: string) => {
    setName(next);
    if (next.trim()) localStorage.setItem(storageKey, next.trim());
    else localStorage.removeItem(storageKey);
  };
  return [name, update];
}
