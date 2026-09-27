# effect-idioms Oxlint plugin

Oxlint rules that steer tagged values towards Effect's own utilities. This folder is a vendored
copy. The rule code and its RuleTester cases live in the `oxlint-rules` repository under
`plugins/effect-idioms/`. `VENDORED.json` records the source commit and a hash for each file.
Streamsy owns the severities, scopes, and exceptions in `.oxlintrc.effect.json`.

| Rule                 | Reports                                                       | Suggested replacement                                                                                                            |
| -------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `no-tag-comparison`  | `x._tag === "Tag"`, `!==`, `==`, `!=`, and `"_tag" in x`      | `Predicate.isTagged`, `Match.valueTags`, `catchTag`, `Option.isSome`, `Exit.isFailure`, `Result.isFailure`, `Cause.isFailReason` |
| `no-tag-switch`      | `switch (x._tag)`                                             | `Match.valueTags`, `$match`, `Schema.TaggedUnion` `match`                                                                        |
| `no-tag-destructure` | `const { _tag: tag } = x`                                     | Narrow `x` itself with a tag utility                                                                                             |
| `no-tag-literal`     | `{ _tag: "Tag", ... }` values and `_tag = "Tag"` class fields | `Data.taggedEnum`, `Data.TaggedError`, `Schema.TaggedStruct`, `Schema.TaggedError`                                               |

`no-tag-comparison` picks its message from context. It names the built-in guard for Effect's own
tags (`Some`, `None`, `Success`, `Failure`, `Fail`, `Die`, `Interrupt`). It suggests `catchTag` inside
`catch*`, `mapError`, `tapError`, and `orElse` callbacks. It suggests `Predicate.isTagged` for
`while` and `until` retry options and for `(x) => x._tag === "Tag"` callbacks.

These places may still name `_tag` directly, and the rules do not report them:

- type declarations, such as `{ readonly _tag: "Applied" }` and `Extract<Op, { _tag: "Create" }>`;
- schema fields, such as `Schema.Struct({ _tag: Schema.Literal("BadRequest") })`;
- shorthand properties, such as `{ _tag, route }` in a wire decoder;
- values inside `expect(...)`, `toEqual`, `toMatchObject`, and similar test assertions.

`.oxlintrc.json` turns the rules off in test files and `src/testing/`. For a deliberate
exception elsewhere, such as an external API option, use a line disable with a reason:

```ts
// oxlint-disable-next-line effect-idioms/no-tag-literal -- provider option shape, not an Effect value
reasoning: { _tag: "disabled" },
```

## Change a rule

Do not edit the files in this folder. Change the rule and its tests in `oxlint-rules`, run
`bun run check` there, then copy it back with `bun run vendor effect-idioms <streamsy-worktree>`.
Run `bun run lint:effect` and `bun run lint:policy` here after each vendor run.
