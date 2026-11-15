---
packages:
  "@streamsy/projection": minor
---

## Read the offset of each local projection entry

`Slice.offsets` now gives the storage offset of every item on local inputs, and
`Entry.offset` gives the corresponding offset in entry handlers. Reading from
`entry.offset` resumes after that entry. Pinned retries retain the same offsets.
Non-empty HTTP inputs have `offsets` undefined and entries omit `offset`, because
the protocol supplies page boundaries only. Every empty slice has `offsets: []`,
including skipped inputs and timer-only units.
See [slices and entries](https://streamsy.dev/docs/reference/projection/units#slice).
