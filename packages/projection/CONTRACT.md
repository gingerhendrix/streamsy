# Contract

`@streamsy/projection` runs a named, typed, checkpointed consumer over one or
more input streams. `Projection.make` and `Projection.stream` return inert
values; neither acquires a service. A projection is identified by `id`,
`generation` (default 1) and `params` (default `{}`). Its checkpoint identity
records the id and content type of every declared input, keyed by input name.
A stored identity that differs from the declaration fails `load` with
`identity-mismatch`. There is no reset: change the declaration under a new
generation.

## Units

Each pass reads every input in declaration order, carrying the remaining item
budget forward. Every declared input has a slice in the batch; an empty slice
has `from === nextOffset`. A unit contains only the non-empty ranges, and its
`key` comes from those ranges, so it is stable across retries. Reads stay
within `units` (per run), `items` and optional `bytes` (per pass). A slice that
exceeds the remaining bytes is refused whole: before any input contributed the
pass reports `limit-reached` without a write and reads no later input; when an
earlier input already contributed, the pass commits that input and leaves the
refused one at its offset. A non-empty pass reports `progress`; only an empty
pass reports `caught-up`, or `source-closed` when every input is closed and
drained.

## Fused form

The handler of `Projection.make` runs on the owner fiber inside the
`Checkpoints` transaction. Writes through that same owner (a stream on the
same storage, SQL through the shared client) commit or roll back together with
the checkpoint. A handler failure or interruption rolls everything back. Writes
to anything outside the owner are at-least-once. The record is compare-and-set:
the token loaded before the read is checked again inside the transaction, and a
concurrent writer fails `checkpoint` / `token-conflict` without advancing.

## Stream form

The handler of `Projection.stream` returns the items to append to `output`
under a producer tuple. Before the append, the record pins
`{ from, nextOffset, count }` per non-empty input together with `seq`; a
conflict on that save is a `pin` failure. The producer epoch equals the
generation and `seq` starts at zero; the producer id is `id` alone, or
`id/canonicalParamsJson` when parameterised. Empty output advances the
checkpoint without a pin or a sequence.

Retry rules:

1. A pending pin is settled before new reads. The retry reproduces exactly
   `count` items after `from` per pinned input, across as many pages as the
   backend answers, and verifies the range ends at `nextOffset`.
2. The retry sends the reproduced unit under exactly the pinned tuple.
   `Appended` and `Duplicate` are one outcome; both advance every pinned input
   to its `nextOffset` and consume the sequence.
3. A range that cannot be reproduced fails `pin` / `range-unreproducible`
   naming the input. Recovery is a new generation.
4. The protocol compares tuples, not payloads. The handler must be
   deterministic on its inputs for byte-identical retries; when it is not, the
   first acknowledged payload stands and the checkpoint still advances.
5. The record stores no output offset.

No unpinned append mode is exposed. A plain at-least-once append of Durable
State `upsert` and `delete` items would converge under replay; the pin exists
so that every output stream, not only convergent ones, sees each unit once.

## Multiple inputs

Progress offsets, identity and pending ranges are maps keyed by input name. A
closed input that is drained stays in every batch as an empty closed slice.
Inputs are read one after another; the package makes no consistent-cut claim
across inputs, and offers no ordering combinator beyond declaration order.

## Faults

`ProjectionFault` carries `phase` (`load`, `read`, `pin`, `process`,
`checkpoint`), `reason`, an optional `input`, and a message. Handler errors are
not wrapped. A `checkpoint` failure means processing completed before the save
failed. No implicit retry or reset occurs.

## Hosts

`Checkpoints` is a Layer-provided service with `load`, compare-and-set `save`
and `withTransaction`. Both shipped Layers, memory and SQLite, have a real
owner transaction; a store without one must not host a fused projection.
Source history after every accepted offset and every pending pinned range must
be retained; missing history is a typed failure. One runner per key is the
supported shape; a second runner is detected by token conflict, not fenced.

`follow` returns a caller-scoped fiber. Each cycle runs to completion, then
races one bounded `readNext` hint per input, repairs missed wakes by timeout,
paces a unit the budget cannot fit, and stops when every input closes and
drains. A closed and drained input never wins that race; the open inputs and
the timeout do. Defaults: 100 units, 1000 items, repair every 1000 ms.
