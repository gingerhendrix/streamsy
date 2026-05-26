# Memory server HTTP walkthrough

This example runs Streamsy's Fetch-based HTTP facade with the in-memory storage backend. It is useful for trying the Durable Streams HTTP surface with `curl` before wiring a persistent storage adapter or application-specific routing.

The memory backend is process-local and non-persistent. Restarting the server clears all streams.

## Start the server

From the repository root:

```bash
bun install
PORT=1337 bun run --cwd examples/memory-server start
```

The server listens on `http://localhost:${PORT:-1337}` and mounts Streamsy at `/`, so each path after the leading slash is treated as the stream id.

In another terminal, set a base URL and stream id for the examples:

```bash
BASE=http://localhost:1337
STREAM=walkthrough/demo
```

## Create a stream

Create an empty JSON stream:

```bash
curl -i -X PUT "$BASE/$STREAM" \
  -H 'Content-Type: application/json'
```

Expected response:

```http
HTTP/1.1 201 Created
content-type: application/json
location: http://localhost:1337/walkthrough/demo
stream-next-offset: 0000000000000000_0000000000000000
```

`Stream-Next-Offset` values are opaque strings. The memory server currently returns padded offsets such as `0000000000000001_0000000000000000`; use the returned header values rather than constructing offsets yourself.

Creating the same stream again with compatible configuration is idempotent and returns `200 OK`:

```bash
curl -i -X PUT "$BASE/$STREAM" \
  -H 'Content-Type: application/json'
```

A conflicting create, such as the same stream id with a different content type, returns `409 Conflict`.

You can also create with initial data. The first message is written atomically with stream creation:

```bash
curl -i -X PUT "$BASE/walkthrough/with-initial" \
  -H 'Content-Type: application/json' \
  --data '{"type":"created"}'
```

## Append data

Append JSON messages by posting each message body to the stream with the stream content type:

```bash
curl -i -X POST "$BASE/$STREAM" \
  -H 'Content-Type: application/json' \
  --data '{"type":"hello","n":1}'
```

Expected response headers include the next offset. A normal non-producer append returns `204 No Content` with no response body:

```http
HTTP/1.1 204 No Content
stream-next-offset: 0000000000000001_0000000000000000
```

Append another message and capture the current offset from the response:

```bash
CURRENT_OFFSET=$(curl -sS -i -X POST "$BASE/$STREAM" \
  -H 'Content-Type: application/json' \
  --data '{"type":"hello","n":2}' \
  | awk -F': ' 'tolower($1)=="stream-next-offset" {gsub("\r", "", $2); print $2}')

echo "$CURRENT_OFFSET"
```

Expected current offset after two appends is similar to `0000000000000002_0000000000000000`.

Optional append headers:

| Header                                          | Use                                                                     |
| ----------------------------------------------- | ----------------------------------------------------------------------- |
| `Stream-Seq`                                    | Application sequence conflict check. A mismatch returns `409 Conflict`. |
| `Stream-Closed: true`                           | Close the stream atomically with this append.                           |
| `Producer-Id`, `Producer-Epoch`, `Producer-Seq` | Producer idempotency metadata. All three must be present together.      |

## Read from initial and current offsets

Read all messages after the initial offset (`-1`):

```bash
curl -i "$BASE/$STREAM?offset=-1"
```

For JSON streams, Streamsy returns a JSON array assembled from the stored JSON messages:

```http
HTTP/1.1 200 OK
content-type: application/json
stream-next-offset: 0000000000000002_0000000000000000
stream-up-to-date: true
etag: "<validator>"

[{"type":"hello","n":1},{"type":"hello","n":2}]
```

Read from a current offset to receive only later messages. If there are no later messages, the body is an empty JSON array and `stream-next-offset` stays at the requested current offset:

```bash
curl -i "$BASE/$STREAM?offset=$CURRENT_OFFSET"
```

Expected response:

```http
HTTP/1.1 200 OK
content-type: application/json
stream-next-offset: 0000000000000002_0000000000000000
stream-up-to-date: true

[]
```

You can also use `offset=now` for a catch-up read that starts at the server's current offset without returning existing messages:

```bash
curl -i "$BASE/$STREAM?offset=now"
```

### Conditional catch-up reads

Catch-up reads include an `ETag`. Reusing the ETag with `If-None-Match` returns `304 Not Modified` while the stream has not advanced for that read range:

```bash
ETAG=$(curl -sS -i "$BASE/$STREAM?offset=-1" \
  | awk -F': ' 'tolower($1)=="etag" {gsub("\r", "", $2); print $2}')

curl -i "$BASE/$STREAM?offset=-1" \
  -H "If-None-Match: $ETAG"
```

## Live reads with long-poll

Long-poll reads wait for messages after the supplied offset:

```bash
curl -i "$BASE/$STREAM?offset=$CURRENT_OFFSET&live=long-poll"
```

If no message arrives before the server-side timeout, the response is `204 No Content` with cursor and state headers:

```http
HTTP/1.1 204 No Content
stream-next-offset: 0000000000000002_0000000000000000
stream-up-to-date: true
stream-cursor: <opaque cursor>
```

To see a successful long-poll, start the long-poll in one terminal and append in another before it times out:

```bash
# terminal 1
curl -i "$BASE/$STREAM?offset=$CURRENT_OFFSET&live=long-poll"

# terminal 2
curl -i -X POST "$BASE/$STREAM" \
  -H 'Content-Type: application/json' \
  --data '{"type":"hello","n":3}'
```

The long-poll response returns the new message and advances the offset:

```http
HTTP/1.1 200 OK
content-type: application/json
stream-next-offset: 0000000000000003_0000000000000000
stream-up-to-date: true
stream-cursor: <opaque cursor>

[{"type":"hello","n":3}]
```

Use the returned `Stream-Cursor` value as the optional `cursor` query parameter on the next live request if you want to carry live-read cursor state forward:

```bash
curl -i "$BASE/$STREAM?offset=$NEXT_OFFSET&live=long-poll&cursor=$CURSOR"
```

## Server-sent events (SSE)

SSE uses the same stream offsets but keeps a `text/event-stream` response open. Start at `offset=-1` to receive existing messages and then control events:

```bash
curl -N "$BASE/$STREAM?offset=-1&live=sse"
```

For JSON streams, data events contain a JSON array payload split across SSE `data:` lines, followed by control events with the next offset and cursor:

```text
event: data
data:[
data:{"type":"hello","n":1},
data:{"type":"hello","n":2}
data:]

event: control
data:{"streamNextOffset":"0000000000000002_0000000000000000","streamCursor":"<opaque cursor>","upToDate":true}
```

If you append while the SSE request is open, the server emits another `data` event and a later `control` event. SSE connections are intentionally finite in this example server; reconnect with the latest `streamNextOffset`/`streamCursor` from the control event.

For non-text and non-JSON streams, SSE data is base64 encoded and the response includes `Stream-SSE-Data-Encoding: base64`.

## Metadata, close, and delete

Use `HEAD` to read stream metadata without a body:

```bash
curl -i -X HEAD "$BASE/$STREAM"
```

Relevant headers include:

```http
content-type: application/json
stream-next-offset: 0000000000000003_0000000000000000
stream-ttl: <seconds, when set>
stream-expires-at: <timestamp, when set>
stream-closed: true    # only when closed
```

Close a stream by appending with `Stream-Closed: true`. The body may be a final message or empty if the append is only closing the stream:

```bash
curl -i -X POST "$BASE/$STREAM" \
  -H 'Content-Type: application/json' \
  -H 'Stream-Closed: true' \
  --data '{"type":"closed"}'
```

Appending to a closed stream returns `409 Conflict` with `Stream-Closed: true` and the current `Stream-Next-Offset`.

Delete a stream:

```bash
curl -i -X DELETE "$BASE/$STREAM"
```

After deletion, reads and appends return `404 Not Found` for the memory backend. Other lifecycle states may surface as `410 Gone` when a storage/runtime keeps a soft-deleted record.

## Header reference

Common request headers:

| Header                                          | Methods       | Meaning                                                                                |
| ----------------------------------------------- | ------------- | -------------------------------------------------------------------------------------- |
| `Content-Type`                                  | `PUT`, `POST` | Stream content type on create; required on non-empty append and must match the stream. |
| `Stream-TTL`                                    | `PUT`         | Non-negative TTL in seconds. Cannot be combined with `Stream-Expires-At`.              |
| `Stream-Expires-At`                             | `PUT`         | Absolute expiry timestamp. Cannot be combined with `Stream-TTL`.                       |
| `Stream-Forked-From`                            | `PUT`         | Source stream id/path when creating a fork.                                            |
| `Stream-Fork-Offset`                            | `PUT`         | Source offset for the fork; requires `Stream-Forked-From`.                             |
| `Stream-Seq`                                    | `POST`        | Optional application sequence check.                                                   |
| `Stream-Closed`                                 | `PUT`, `POST` | `true` creates/closes the stream as closed.                                            |
| `Producer-Id`, `Producer-Epoch`, `Producer-Seq` | `POST`        | Optional idempotent producer metadata; all three are required if any are present.      |
| `If-None-Match`                                 | `GET`         | Conditional catch-up read using a previous `ETag`.                                     |

Common response headers:

| Header                                           | Meaning                                                                                         |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `Stream-Next-Offset`                             | Offset to use for the next catch-up or live read. `-1` means no messages have been written yet. |
| `Stream-Cursor`                                  | Opaque live-read cursor for subsequent long-poll/SSE requests.                                  |
| `Stream-Up-To-Date`                              | `true` when the response has caught up to the stream at response time.                          |
| `Stream-Closed`                                  | `true` when the stream is closed, or when an append failed because the stream is closed.        |
| `ETag`                                           | Validator for catch-up GET responses. Use with `If-None-Match`.                                 |
| `Location`                                       | Created stream URL on `201 Created`.                                                            |
| `Producer-Epoch`, `Producer-Seq`                 | Producer state returned for idempotent appends/duplicates.                                      |
| `Producer-Expected-Seq`, `Producer-Received-Seq` | Details for producer sequence-gap conflicts.                                                    |
| `Stream-SSE-Data-Encoding`                       | `base64` when SSE data payloads are binary encoded.                                             |

Responses also include security headers such as `X-Content-Type-Options: nosniff` and `Cross-Origin-Resource-Policy: cross-origin`.

## Common status and error mapping

| Status                      | Typical cause                                                                                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `200 OK`                    | Compatible create of an existing stream; producer-tracked non-empty append; catch-up/live read with messages; metadata `HEAD`.                                     |
| `201 Created`               | New stream created.                                                                                                                                                |
| `204 No Content`            | Successful ordinary append/close with no response body, idempotent duplicate append, successful long-poll timeout/no messages, or successful delete.               |
| `304 Not Modified`          | Catch-up `GET` with a matching `If-None-Match` ETag.                                                                                                               |
| `400 Bad Request`           | Missing stream path, invalid offset, invalid JSON, empty append without close, missing content type on non-empty append, invalid TTL/expiry/fork/producer headers. |
| `403 Forbidden`             | Stale producer epoch.                                                                                                                                              |
| `404 Not Found`             | Stream or fork source does not exist.                                                                                                                              |
| `405 Method Not Allowed`    | Unsupported HTTP method.                                                                                                                                           |
| `409 Conflict`              | Content-type mismatch, sequence conflict, incompatible create, append to closed stream, or producer sequence gap.                                                  |
| `410 Gone`                  | Stream is in a soft-deleted lifecycle state. The memory backend's `DELETE` currently removes the record, so later reads commonly return `404 Not Found`.           |
| `413 Payload Too Large`     | Create/append body exceeds `maxMessageSize` (default 1 MiB).                                                                                                       |
| `500 Internal Server Error` | Unexpected server error.                                                                                                                                           |

## Practical notes

- Offsets are strings. Use `-1` as the initial read offset and the returned `Stream-Next-Offset` for subsequent reads.
- Query offsets must be `-1`, `now`, or an offset in the form `<number>_<number>`.
- JSON stream reads return arrays because HTTP may combine multiple stored JSON messages into one response body.
- App-specific concerns such as auth, CORS, write validation, and read-only policy should wrap the HTTP handler rather than be implemented in this memory-server example.
