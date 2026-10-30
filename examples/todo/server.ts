/**
 * A todo app in one file.
 *
 * - Each todo list is one Streamsy JSON event stream, `todo-lists/:listId`.
 * - `POST`, `PATCH` and `PUT` routes are commands. Each one folds the list,
 *   decides, and appends events with `Projection.decide`.
 * - One fold projection for each list holds the materialized list.
 *   `GET /lists/:listId` serves it through `Projection.current`, so a read
 *   after a write sees that write.
 * - `Projection.attach` keeps each list's checkpoint current after every append.
 *
 * Run it with `bun server.ts`. Set `TODO_DB` to a file name to keep the
 * streams and checkpoints in SQLite.
 */
import { BunCrypto, BunRuntime } from "@effect/platform-bun";
import { MemoryStorage, Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { BunStorage } from "@streamsy/storage/bun";
import { Array, Config, Crypto, DateTime, Effect, Layer, Match, Option, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse, HttpServerError } from "effect/http";

// ---------------------------------------------------------------------------
// Events: what happened to a list. Each list has its own stream.
// ---------------------------------------------------------------------------

/** A list id or todo id in a path: letters, digits, `-` and `_`. */
const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,64}$/));

const Title = Schema.NonEmptyString.check(Schema.isMaxLength(200));

export const TodoStatus = Schema.Literals(["todo", "doing", "done"]);

export type TodoStatus = typeof TodoStatus.Type;

/** The server stamps `at`, so a replay of the stream gives the same list. */
export const TodoEvent = Schema.TaggedUnion({
  TodoAdded: { todoId: Schema.String, title: Title, at: Schema.String },
  TodoEdited: { todoId: Schema.String, title: Title, at: Schema.String },
  TodoStatusChanged: { todoId: Schema.String, status: TodoStatus, at: Schema.String },
});

export type TodoEvent = typeof TodoEvent.Type;

const { TodoAdded, TodoEdited, TodoStatusChanged } = TodoEvent.cases;

/** One event stream for each list. The first add creates it. */
export class TodoEvents extends Streamsy.json<TodoEvents>()(
  "Todo/TodoEvents",
  "todo-lists/:listId",
  {
    params: { listId: Id },
    schema: TodoEvent,
  },
) {}

export const streams = Streamsy.group(TodoEvents);

// ---------------------------------------------------------------------------
// Projection: the materialized list, folded from its events.
// ---------------------------------------------------------------------------

export const Todo = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  status: TodoStatus,
  createdAt: Schema.String,
  updatedAt: Schema.String,
});

export type Todo = typeof Todo.Type;

export const TodoList = Schema.Struct({ listId: Schema.String, todos: Schema.Array(Todo) });

export type TodoList = typeof TodoList.Type;

/** The todo that one `TodoAdded` event creates. */
const addedTodo = ({ todoId, title, at }: typeof TodoAdded.Type): Todo => ({
  id: todoId,
  title,
  status: "todo",
  createdAt: at,
  updatedAt: at,
});

/** The list after one event. An edit of a todo that the list does not hold changes nothing. */
function applyEvent(list: TodoList, event: TodoEvent): TodoList {
  const update = (todoId: string, change: (todo: Todo) => Todo): TodoList => ({
    ...list,
    todos: Array.map(list.todos, (todo) => (todo.id === todoId ? change(todo) : todo)),
  });

  return Match.valueTags(event, {
    TodoAdded: (added) => ({ ...list, todos: [...list.todos, addedTodo(added)] }),
    TodoEdited: ({ todoId, title, at }) =>
      update(todoId, (todo) => ({ ...todo, title, updatedAt: at })),
    TodoStatusChanged: ({ todoId, status, at }) =>
      update(todoId, (todo) => ({ ...todo, status, updatedAt: at })),
  });
}

/** One materialized list for each `listId`. `todoList.at({ listId })` binds one list. */
export const todoList = Projection.fold({
  id: "todo-list",
  params: { listId: Id },
  inputs: { events: TodoEvents },
  state: { schema: TodoList, initial: ({ listId }) => ({ listId, todos: [] }) },
  fold: (list, { item }) => applyEvent(list, item),
});

// ---------------------------------------------------------------------------
// Commands: fold the list, decide, append at the offset that was read.
// ---------------------------------------------------------------------------

/** An edit or status change names a todo that the list does not hold. */
export class TodoNotFound extends Schema.TaggedError<
  TodoNotFound,
  { readonly brand: unique symbol }
>()("TodoNotFound", { todoId: Schema.String }) {}

const findTodo = (list: TodoList, todoId: string) =>
  Effect.fromOption(
    Array.findFirst(list.todos, (todo) => todo.id === todoId),
    () => new TodoNotFound({ todoId }),
  );

/**
 * Decide one command against the current list and append its events. The
 * append carries the offset that was read, so a lost race folds the new tail
 * and decides again. A failed decision appends nothing.
 */
const decide = <E>(
  listId: string,
  decision: (list: TodoList) => Effect.Effect<{ todo: Todo; events: ReadonlyArray<TodoEvent> }, E>,
) =>
  Projection.decide(todoList.at({ listId }), {
    input: "events",
    decide: ({ state }) =>
      Effect.map(decision(state), ({ todo, events }) => ({ value: todo, items: events })),
  });

const now = Effect.map(DateTime.now, DateTime.formatIso);

/** A list that was never added to has no stream, so `head` fails with `StreamNotFound`. */
const requireList = (listId: string) =>
  Effect.flatMap(TodoEvents, (events) => events.head({ listId }));

const addTodo = Effect.fn("Todo.add")(function* (listId: string, title: string) {
  const events = yield* TodoEvents;
  const crypto = yield* Crypto.Crypto;
  const todoId = (yield* crypto.randomUUIDv4()).slice(0, 8);
  const at = yield* now;

  // The first add creates the list's stream. A second create of the same stream succeeds.
  yield* events.create({ listId });

  const added = TodoAdded.make({ todoId, title, at });

  // An add has no rule to check, so it always appends.
  return yield* decide(listId, () => Effect.succeed({ todo: addedTodo(added), events: [added] }));
});

const editTodo = Effect.fn("Todo.edit")(function* (listId: string, todoId: string, title: string) {
  yield* requireList(listId);
  const at = yield* now;

  return yield* decide(listId, (list) =>
    Effect.map(findTodo(list, todoId), (todo) =>
      // The same title is no change, so nothing is appended.
      todo.title === title
        ? { todo, events: [] }
        : {
            todo: { ...todo, title, updatedAt: at },
            events: [TodoEdited.make({ todoId, title, at })],
          },
    ),
  );
});

const setStatus = Effect.fn("Todo.setStatus")(function* (
  listId: string,
  todoId: string,
  status: TodoStatus,
) {
  yield* requireList(listId);
  const at = yield* now;

  return yield* decide(listId, (list) =>
    Effect.map(findTodo(list, todoId), (todo) =>
      todo.status === status
        ? { todo, events: [] }
        : {
            todo: { ...todo, status, updatedAt: at },
            events: [TodoStatusChanged.make({ todoId, status, at })],
          },
    ),
  );
});

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------

const ListParams = Schema.Struct({ listId: Id });

const TodoParams = Schema.Struct({ listId: Id, todoId: Id });

const AddBody = Schema.Struct({ title: Title });

const EditBody = Schema.Struct({ title: Title });

const StatusBody = Schema.Struct({ status: TodoStatus });

/** `GET /lists/:listId`: the materialized list and the event offset it includes. */
const ListResponse = Schema.Struct({ ...TodoList.fields, offset: Schema.String });

const json = HttpServerResponse.jsonUnsafe;

// Each route answers its expected failures. Any other failure reaches the
// router, which answers 500 (or the status of a Streamsy serve error).

const badRequest = (error: Schema.SchemaError | HttpServerError.HttpServerError) =>
  Effect.succeed(json({ error: `Invalid request: ${error.message}` }, { status: 400 }));

const listNotFound = () => Effect.succeed(json({ error: "List not found" }, { status: 404 }));

const todoNotFound = ({ todoId }: TodoNotFound) =>
  Effect.succeed(json({ error: `Todo ${todoId} not found` }, { status: 404 }));

/** `Projection.decide` retried three lost races and still lost. */
const conflict = () =>
  Effect.succeed(json({ error: "Concurrent writes, please retry" }, { status: 409 }));

/** The materialized list: the checkpoint plus every event appended after it. */
const getList = Effect.gen(function* () {
  const { listId } = yield* HttpRouter.schemaPathParams(ListParams);
  yield* requireList(listId);

  return yield* Serve.value(
    ListResponse,
    Effect.map(Projection.current(todoList.at({ listId })), ({ state, offsets }) => ({
      ...state,
      offset: offsets.events,
    })),
  );
}).pipe(Effect.catchTags({ SchemaError: badRequest, StreamNotFound: listNotFound }));

const getTodo = Effect.gen(function* () {
  const { listId, todoId } = yield* HttpRouter.schemaPathParams(TodoParams);
  yield* requireList(listId);
  const { state } = yield* Projection.current(todoList.at({ listId }));

  return json({ todo: yield* findTodo(state, todoId) });
}).pipe(
  Effect.catchTags({
    SchemaError: badRequest,
    StreamNotFound: listNotFound,
    TodoNotFound: todoNotFound,
  }),
);

/** The saved checkpoint alone. `Projection.attach` moves it forward after each append. */
const getCheckpoint = Effect.gen(function* () {
  const { listId } = yield* HttpRouter.schemaPathParams(ListParams);
  const { state, offsets } = yield* Projection.read(todoList.at({ listId }));

  return json({ offset: offsets.events, todos: state.todos.length });
}).pipe(Effect.catchTags({ SchemaError: badRequest }));

const postTodo = Effect.gen(function* () {
  const { listId } = yield* HttpRouter.schemaPathParams(ListParams);
  const { title } = yield* HttpServerRequest.schemaBodyJson(AddBody);
  const { value, offset } = yield* addTodo(listId, title);

  return json({ todo: value, offset }, { status: 201 });
}).pipe(
  Effect.catchTags({
    SchemaError: badRequest,
    HttpServerError: badRequest,
    OffsetMismatch: conflict,
  }),
);

const patchTodo = Effect.gen(function* () {
  const { listId, todoId } = yield* HttpRouter.schemaPathParams(TodoParams);
  const { title } = yield* HttpServerRequest.schemaBodyJson(EditBody);
  const { value, offset, appended } = yield* editTodo(listId, todoId, title);

  return json({ todo: value, offset, appended });
}).pipe(
  Effect.catchTags({
    SchemaError: badRequest,
    HttpServerError: badRequest,
    StreamNotFound: listNotFound,
    TodoNotFound: todoNotFound,
    OffsetMismatch: conflict,
  }),
);

const putStatus = Effect.gen(function* () {
  const { listId, todoId } = yield* HttpRouter.schemaPathParams(TodoParams);
  const { status } = yield* HttpServerRequest.schemaBodyJson(StatusBody);
  const { value, offset, appended } = yield* setStatus(listId, todoId, status);

  return json({ todo: value, offset, appended });
}).pipe(
  Effect.catchTags({
    SchemaError: badRequest,
    HttpServerError: badRequest,
    StreamNotFound: listNotFound,
    TodoNotFound: todoNotFound,
    OffsetMismatch: conflict,
  }),
);

export const Routes = Layer.mergeAll(
  HttpRouter.add("POST", "/lists/:listId/todos", postTodo),
  HttpRouter.add("PATCH", "/lists/:listId/todos/:todoId", patchTodo),
  HttpRouter.add("PUT", "/lists/:listId/todos/:todoId/status", putStatus),
  HttpRouter.add("GET", "/lists/:listId", getList),
  HttpRouter.add("GET", "/lists/:listId/todos/:todoId", getTodo),
  HttpRouter.add("GET", "/lists/:listId/checkpoint", getCheckpoint),
  // The raw event stream of a list, read-only, over the Durable Streams protocol.
  Serve.mount(TodoEvents, "/streams/todo-lists/:listId"),
);

// ---------------------------------------------------------------------------
// Storage and server
// ---------------------------------------------------------------------------

/** A memory store. Checkpoints live on the same owner as the streams. */
export const MemoryHost = CheckpointStore.layer.pipe(
  Layer.provideMerge(MemoryStorage.layer(streams)),
);

/** A Bun SQLite store in `filename`. A restart resumes each checkpoint. */
export const sqliteHost = (filename: string) =>
  CheckpointStore.layer.pipe(
    Layer.provideMerge(BunStorage.layer(streams, { client: { filename } })),
  );

/**
 * The routes over a host. `Layer.provide` builds the attached runner before the
 * routes, so it subscribes to commits before the first write.
 */
export const app = <E>(host: Layer.Layer<Layer.Success<typeof MemoryHost>, E>) =>
  Routes.pipe(
    // The handlers read the stream and checkpoint services for each request.
    HttpRouter.provideRequest(
      Layer.effectContext(Effect.context<TodoEvents | CheckpointStore | Crypto.Crypto>()),
    ),
    Layer.provide(Projection.attach(todoList, { durable: true })),
    Layer.provide([host, BunCrypto.layer]),
  );

const Host = Layer.unwrap(
  Effect.map(Config.option(Config.String("TODO_DB")), (filename) =>
    Option.match(filename, { onNone: () => MemoryHost, onSome: sqliteHost }),
  ),
);

const Listener = Layer.unwrap(
  Effect.map(Config.Port("PORT").pipe(Config.withDefault(3000)), (port) => listener({ port })),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(Serve.layer(app(Host)).pipe(Layer.provide(Listener))), {
    teardown: Serve.teardown,
  });
}
