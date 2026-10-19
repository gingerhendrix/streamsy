# Issue tracker demo contract

The wire contract that the [issue tracker demo](../issue-tracker-demo) and its
variants share. It is a private workspace package. It is not published.

| Module         | Holds                                                                     |
| -------------- | ------------------------------------------------------------------------- |
| `state-schema` | The row schemas, `wipLimit`, the StreamDB state schema, the event decoder |
| `api-schema`   | The request and response bodies and the error classes of the JSON API     |
| `api`          | `IssueTrackerApi`, the `HttpApi` declaration                              |
| `types`        | Type-only exports for the browser                                         |

`api-schema` imports only `Schema`, so the browser can decode responses
without the `HttpApi` modules. Import each module by its subpath, for example
`@streamsy/issue-tracker-demo-contract/api-schema`.

A variant keeps this contract. A change here changes every variant.
