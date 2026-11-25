---
packages:
  "@streamsy/projection": minor
  "@streamsy/views": minor
---

## Use `generation` instead of `version` on projections and Views circuits

**Breaking change:** Projection and Views circuit declarations now use `generation` alone to version stored state and rebuilds, and reject any defined `version` with `RangeError`.

- **Declarations:** Remove `version: 1` to keep the existing generation. For any other version, remove `version` and raise `generation` above every generation the `id` has previously used.
- **Types and custom stores:** `ProjectionKey`, `Unit`, and the derived declaration and binding types no longer have a `version` field. If custom stores or handlers included `key.version` or `unit.version` in storage keys, substitute the literal `1` to preserve keys for the former default version. Passing a key object with a defined `version` other than `1` to `recordKey`, `Projection.forget`, or a `fromStore` checkpoint store throws `RangeError`.
- **Unchanged:** `View.fn(name, version, implementation)` keeps its function version argument.

When upgrading a declaration that used a `version` other than `1`, stop the old runners, retain full input history, and prepare output addresses before starting the upgraded process. Let the new generation catch up and move readers before retiring the old binding with a one-off 0.13.0 script that only calls `retire`. See [Migrate a declaration that set `version`](https://streamsy.dev/docs/projections/checkpoints#migrate-a-declaration-that-set-version).
