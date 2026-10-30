---
packages:
  "group:streamsy": patch
---

## The Cloudflare object host logs requests

`StreamsyObject.make` and `objectHandlers` built their web handler with `disableLogger: true`. A handler defect answered 500 and logged nothing, so the only sign of the failure was the status code.

- The object host now logs each request at INFO level when it ends, with method, URL and status.
- A failed request logs its full cause, stack included.
- Each request adds one line to Workers logs.
