/**
 * Docs pages that moved: the old slug path under `/docs`, and the slug path of the page
 * that replaced it. The docs route answers an old path with a 308 redirect, and
 * `check:links` checks each entry.
 */
export const movedPages = new Map([["demos/issue-tracker-demo", "demos/issue-tracker"]]);
