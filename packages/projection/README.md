# @streamsy/projection

Checkpointed Effect projections over one or more retained streams.

A projection is a named, typed, checkpointed consumer of protocol batches from
one or more input streams. It processes one unit at a time through an Effect,
and commits the handler's local writes together with the checkpoint in one
owner transaction.

The package is under construction on this branch. The full README, contract,
and SQLite Layer follow in later commits.
