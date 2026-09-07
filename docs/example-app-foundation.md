# Example application foundation

The active applications are Fold agent and Hacker News. Both compose the
Effect protocol and own their runtime at an executable edge.
Their evidence informs later library work without claiming capabilities that the
current store cannot provide.

## Fold agent

`examples/fold-agent` persists exact pending payloads and producer tuples into a
session journal in one acquired memory or Bun SQLite store. Reconstruction settles pending
work before new input or epoch takeover, checks full acknowledged payload equality
and uses journal expected-offset CAS for ownership. Its tests cover ambiguous
completion and fencing. Separate journal/log reads can safely reject during
concurrent writes; callers may retry reconstruction. Retained-file tests run the
writer, restart, ambiguous-completion recovery, takeover, and CLI commands in
separate Bun processes without provider credentials or network calls.

## Hacker News

`examples/hackernews-newest-stream` runs the memory Bun host, an example-local
projection bridge and the official browser State binding. Root units include its
12 tests; `bun run smoke:hackernews` checks the offline HTTP path.

The bridge commits one whole source read page as one output boundary. A backlog
above the item/byte limit can repeatedly return `boundary-too-large`; it does not
split that page. This accepted bounded-demo limitation remains through Step 1.
Recovery scans target fact headers and uses target-tail CAS. Owner fencing,
generation activation and replay-safe pending writes remain later derive work.

## Historical portfolio and working cycle

The issue tracker variants, Risk demo and memory server are parked outside root
workspace inputs; see the [inventory](../parked/README.md). They are historical
references, not current executable API examples. Hex Domination remains product
research rather than a Step 1 compatibility proof.

For each new slice, state product behavior and persistence/failure boundaries,
implement the smallest complete application path, verify recovery claims, and
record awkward APIs before extracting shared primitives. Review source positions,
output commit boundaries, resource lifetime and test evidence together. Keep one
writer per worktree and commit coherent slices; deployment and release decisions
remain separate. Do not infer replay safety from a passing happy-path demo.
