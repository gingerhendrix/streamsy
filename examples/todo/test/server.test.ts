import { BunHttpServer } from "@effect/platform-bun";
import { it } from "@effect/vitest";
import { afterAll, describe, expect } from "vitest";
import { Effect, Layer, Schedule, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { app, MemoryHost, Todo, TodoList } from "../server.ts";

// The server's routes over a memory store, served in process. One handler for
// the whole file, so every test uses its own list id.
const { handler, dispose } = HttpRouter.toWebHandler(
  app(MemoryHost).pipe(Layer.provide(BunHttpServer.layerHttpServices)),
  { disableLogger: true },
);

afterAll(dispose);

const Written = Schema.Struct({
  todo: Todo,
  offset: Schema.String,
  appended: Schema.optionalKey(Schema.Boolean),
});

const ListBody = Schema.Struct({ ...TodoList.fields, offset: Schema.String });

const ErrorBody = Schema.Struct({ error: Schema.String });

const Checkpoint = Schema.Struct({ offset: Schema.String, todos: Schema.Int });

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));

/** Send one request and decode the JSON answer with `schema`. */
const send = <A>(schema: Schema.Codec<A>, method: string, path: string, body?: Schema.Json) =>
  Effect.gen(function* () {
    const encoded = body === undefined ? undefined : yield* encodeJson(body);

    const response = yield* Effect.promise(() =>
      handler(
        new Request(`http://localhost${path}`, {
          method,
          headers: { "content-type": "application/json" },
          body: encoded,
        }),
      ),
    );

    const json = yield* Effect.promise(() => response.json());

    return { status: response.status, body: yield* Schema.decodeUnknownEffect(schema)(json) };
  });

const addTodo = (listId: string, title: string) =>
  send(Written, "POST", `/lists/${listId}/todos`, { title });

const getList = (listId: string) => send(ListBody, "GET", `/lists/${listId}`);

describe("todo commands", () => {
  it.live("an add appends TodoAdded and the list serves the new todo", () =>
    Effect.gen(function* () {
      const added = yield* addTodo("add", "Buy milk");

      expect(added.status).toBe(201);
      expect(added.body.todo).toMatchObject({ title: "Buy milk", status: "todo" });

      const list = yield* getList("add");

      expect(list.status).toBe(200);
      expect(list.body.offset).toBe(added.body.offset);
      expect(list.body).toMatchObject({ listId: "add", todos: [added.body.todo] });
    }),
  );

  it.live("an edit changes the title, and the same title appends nothing", () =>
    Effect.gen(function* () {
      const { body: added } = yield* addTodo("edit", "Buy milk");
      const path = `/lists/edit/todos/${added.todo.id}`;

      const edited = yield* send(Written, "PATCH", path, { title: "Buy oat milk" });

      expect(edited.status).toBe(200);
      expect(edited.body).toMatchObject({ appended: true, todo: { title: "Buy oat milk" } });

      const again = yield* send(Written, "PATCH", path, { title: "Buy oat milk" });

      expect(again.body).toMatchObject({ appended: false, offset: edited.body.offset });

      const list = yield* getList("edit");

      expect(list.body.todos).toEqual([edited.body.todo]);
    }),
  );

  it.live("a status change moves the todo to the new status", () =>
    Effect.gen(function* () {
      const { body: added } = yield* addTodo("status", "Write tests");
      const path = `/lists/status/todos/${added.todo.id}/status`;

      const doing = yield* send(Written, "PUT", path, { status: "doing" });
      const done = yield* send(Written, "PUT", path, { status: "done" });

      expect(doing.body.todo.status).toBe("doing");
      expect(done.body.todo.status).toBe("done");

      const todo = yield* send(
        Schema.Struct({ todo: Todo }),
        "GET",
        `/lists/status/todos/${added.todo.id}`,
      );

      expect(todo.body.todo).toEqual(done.body.todo);
    }),
  );
});

describe("rejected commands", () => {
  it.live("an edit or status change of an unknown todo answers 404 and appends nothing", () =>
    Effect.gen(function* () {
      const { body: added } = yield* addTodo("unknown", "Only todo");

      const edit = yield* send(ErrorBody, "PATCH", "/lists/unknown/todos/missing", {
        title: "x",
      });

      const status = yield* send(ErrorBody, "PUT", "/lists/unknown/todos/missing/status", {
        status: "done",
      });

      expect(edit).toEqual({ status: 404, body: { error: "Todo missing not found" } });
      expect(status).toEqual({ status: 404, body: { error: "Todo missing not found" } });

      const list = yield* getList("unknown");

      expect(list.body.offset).toBe(added.offset);
      expect(list.body.todos).toHaveLength(1);
    }),
  );

  it.live("an unknown list answers 404", () =>
    Effect.gen(function* () {
      expect(yield* send(ErrorBody, "GET", "/lists/never")).toEqual({
        status: 404,
        body: { error: "List not found" },
      });

      expect((yield* send(ErrorBody, "PATCH", "/lists/never/todos/a", { title: "x" })).status).toBe(
        404,
      );
    }),
  );

  it.live("a body that fails its schema answers 400", () =>
    Effect.gen(function* () {
      const { body: added } = yield* addTodo("invalid", "Valid");

      expect((yield* send(ErrorBody, "POST", "/lists/invalid/todos", { title: "" })).status).toBe(
        400,
      );

      expect(
        (yield* send(ErrorBody, "PUT", `/lists/invalid/todos/${added.todo.id}/status`, {
          status: "later",
        })).status,
      ).toBe(400);
    }),
  );
});

describe("checkpoints", () => {
  it.live("the attached projection moves the checkpoint to the head of the list", () =>
    Effect.gen(function* () {
      yield* addTodo("checkpoint", "One");
      const { body: last } = yield* addTodo("checkpoint", "Two");

      // The attached runner saves the checkpoint after the append answers.
      const checkpoint = yield* send(Checkpoint, "GET", "/lists/checkpoint/checkpoint").pipe(
        Effect.repeat({
          until: ({ body }) => body.offset === last.offset,
          schedule: Schedule.spaced("10 millis"),
          times: 300,
        }),
      );

      expect(checkpoint.body).toEqual({ offset: last.offset, todos: 2 });
    }),
  );
});
