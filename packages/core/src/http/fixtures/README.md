# Frozen HTTP wire expectations

`wire.json` holds the recorded HTTP responses (status, headers, and exact
body bytes) that the HTTP tests compare against. It is test data and is not
part of the package build.

There is no regeneration script. A changed expectation is a wire-contract
change and must be reviewed as one. The file was captured at commit `9c86871`
from the pre-Effect implementation, with a fixed wall time of
`1780000000000`; the lowercase ISO and RFC UTC expiry rejection rows are the
one accepted difference and come from the Effect edge.
