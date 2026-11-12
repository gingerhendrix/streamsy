---
packages:
  "@streamsy/projection": patch
---

## Index folder-sync folders as separate triggered bindings

The folder-sync Worker now attaches its search projection with `triggers: [FolderWake]`, replacing the hand-written wake loop. Each folder has its own pending mark and backoff, and unread wake rows are recovered after an index restart without a new source write.
