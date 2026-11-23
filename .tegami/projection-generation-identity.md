---
packages:
  "@streamsy/projection": minor
  "@streamsy/views": minor
---

## Use `generation` to rebuild projections and Views circuits

**Breaking change:** The `version` option is gone from projection and Views circuit declarations. A defined `version` throws `RangeError` at declaration time. `ProjectionKey`, `Unit`, `Identity`, `Declaration`, `Attachable`, bound projections, `StateTable` declarations and bindings, and Views circuit declarations and bindings lose the `version` field. `View.fn(name, version, implementation)` keeps its function version.

Before upgrading, retire bindings on 0.13.0 if they used a version other than `1` and you need to remove their routed streams. After the upgrade, `forget` and `retire` address only the version 1 slot. Records, route records, and Views operator namespaces in other version slots become unreachable by those calls.

If your declaration set `version`, remove the option and set `generation` above every generation that the projection or circuit id has used under any version. For example, if the id only used generation `1` with `version: 3`, set `generation: 2`. Never delete `version` alone. The runtime check cannot see a deleted option, and the declaration can resume an old version 1 checkpoint. [Migrate a declaration that set `version`](https://streamsy.dev/docs/projections/checkpoints#migrate-a-declaration-that-set-version)

An unwrapped `Commit.producer` output receives the replayed history after its old items. Move the output address or delete its stream before replay. [Generation-addressed outputs](https://streamsy.dev/docs/projections/outputs#generation-addressed-outputs)

A circuit that never set `version` needs no change. Its stored data stays in place, with no rebuild required for this upgrade. [Views lifecycle and rebuilds](https://streamsy.dev/docs/views/lifecycle)
