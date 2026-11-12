---
packages:
  "@streamsy/projection": patch
---

## Keep changes subscribers open after snapshot faults

Registry changes now logs and skips failed mark-summary snapshots, including the initial read, and emits the next good snapshot on the same subscription. Its stream error type changes from StorageFault to never. Registry status still fails with StorageFault.
