---
packages:
  "@streamsy/projection": minor
  "@streamsy/views": minor
---

## Use `generation` to rebuild projections and Views circuits

**Breaking change:** Projection and Views circuit declarations reject a defined `version` with `RangeError`. Remove `version: 1` to keep the existing generation. For other versions, raise `generation` above every generation the id has used. `ProjectionKey`, `Unit`, and the derived declaration and binding types lose `version`. If custom stores or handlers used `key.version` or `unit.version` in their keys, substitute the literal `1` to preserve keys for the former default version. `View.fn(name, version, implementation)` keeps its function version.

When moving from another version, stop old runners, keep the full input history, and prepare output addresses before starting the upgraded process. Let the new generation catch up and move readers before retiring the old binding with a one-off 0.13.0 script that only calls `retire`. [Migrate a declaration that set `version`](https://streamsy.dev/docs/projections/checkpoints#migrate-a-declaration-that-set-version)
