# Todo

A todo app in one file, [`server.ts`](server.ts). It is event sourced with Streamsy:

- Each todo list is one JSON event stream, `todo-lists/:listId`, with three events: `TodoAdded`, `TodoEdited` and `TodoStatusChanged`.
- The write routes are commands. Each one folds the list, decides, and appends with `Projection.decide`. An edit or status change of a todo that the list does not hold answers 404 and appends nothing.
- One `Projection.fold` for each list holds the materialized list. `GET /lists/:listId` serves it with `Projection.current`, so a read after a write includes that write.
- `Projection.attach` saves each list's checkpoint after every append, so `Projection.current` folds only the events after the checkpoint.

A todo has a `status` of `todo`, `doing` or `done`. The first add to a list creates its stream.

## Run

```bash
bun install            # from the repo root
bun run build          # from the repo root: builds the @streamsy packages
cd examples/todo
bun server.ts          # memory store, http://127.0.0.1:3000
```

| Variable  | Default | Effect                                                                     |
| --------- | ------- | -------------------------------------------------------------------------- |
| `PORT`    | `3000`  | Listener port                                                              |
| `TODO_DB` | unset   | SQLite file for streams and checkpoints. Unset keeps everything in memory. |

## Endpoints

| Method  | Path                                  | Body                                        | Answer                                                       |
| ------- | ------------------------------------- | ------------------------------------------- | ------------------------------------------------------------ |
| `POST`  | `/lists/:listId/todos`                | `{ "title": string }`                       | 201 `{ todo, offset }`                                       |
| `PATCH` | `/lists/:listId/todos/:todoId`        | `{ "title": string }`                       | 200 `{ todo, offset, appended }`, 404 for an unknown todo    |
| `PUT`   | `/lists/:listId/todos/:todoId/status` | `{ "status": "todo" \| "doing" \| "done" }` | 200 `{ todo, offset, appended }`, 404 for an unknown todo    |
| `GET`   | `/lists/:listId`                      |                                             | 200 `{ listId, offset, todos }`, 404 for an unknown list     |
| `GET`   | `/lists/:listId/todos/:todoId`        |                                             | 200 `{ todo }`                                               |
| `GET`   | `/lists/:listId/checkpoint`           |                                             | 200 `{ offset, todos }`: the saved checkpoint only           |
| `GET`   | `/streams/todo-lists/:listId`         |                                             | The raw event stream, read with the Durable Streams protocol |

`offset` is the event stream offset after the write, or the offset that a read includes. `appended` is `false` when the command changed nothing, such as an edit to the same title. A body that fails its schema answers 400.

```bash
curl -s -X POST localhost:3000/lists/groceries/todos \
  -H 'content-type: application/json' -d '{"title":"Buy milk"}'
# {"todo":{"id":"58fb65cc","title":"Buy milk","status":"todo",...},"offset":"0000000000000001_0000000000000000"}

curl -s -X PATCH localhost:3000/lists/groceries/todos/58fb65cc \
  -H 'content-type: application/json' -d '{"title":"Buy oat milk"}'

curl -s -X PUT localhost:3000/lists/groceries/todos/58fb65cc/status \
  -H 'content-type: application/json' -d '{"status":"done"}'

curl -s localhost:3000/lists/groceries
# {"listId":"groceries","offset":"0000000000000003_0000000000000000","todos":[...]}

curl -s 'localhost:3000/streams/todo-lists/groceries?offset=-1'
# [{"_tag":"TodoAdded",...},{"_tag":"TodoEdited",...},{"_tag":"TodoStatusChanged",...}]
```

## Verify

```bash
bun run typecheck
bun run test
```

The test serves the routes in process over a memory store. It covers add, edit, status change, 404 for an unknown todo and list, 400 for a bad body, and the attached checkpoint.
