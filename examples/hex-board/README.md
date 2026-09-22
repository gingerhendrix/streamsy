# Hex board

A fused `Projection.fold` keeps a complete Hex Domination board and the next
move ordinal in one schema-encoded state row. The SQLite Layer commits that row
with its checkpoint. Read it with `Projection.loadState(board, BoardState)`.

```sh
bun run build
bun run --cwd examples/hex-board test
```

The restart test closes the first runtime during a pending attack, opens the
same SQLite file, checks zero new items and unchanged state/checkpoint, then
appends the rest of the game and compares against a pure fold. Memory exercises
the same incremental fold. The pure tests compare the reducer to the independent
aggregate at every fixture prefix, including pending combat and the 40-move cap.

`test:unit` is discovered by the root workspace test runner. There is no output
stream or application table: this example uses `streamsy_projection_v1_state`
and `streamsy_projection_v1_records` supplied by the projection Layer.

## Fixtures

Both JSON logs were generated once with the historical demo's pure testkit;
replaying them needs no command service, bot, random source or map generator.
`full.json` uses the existing victory driver, seed `projection-equivalence`, two
players and a two-territory losing seat. `long.json` uses four players,
`rngSeed=11`, map seed `seed-11`, and a seeded random legal-action policy for
3,000 decisions: 3,005 events, still playing at the cap. The generator and
measurement scripts are retained in the dated O6 scratch directory described
in the batch review guide. These are generated games, not captured live games.

Each unit gives its events one coarse source watermark. Move ids use the saved
ordinal, so they stay distinct across both batches and restarts. State size is
bounded in game length for a fixed board and bounded event payloads; the map,
roster and recent 40 moves determine its size. Schema changes require a new
generation.
