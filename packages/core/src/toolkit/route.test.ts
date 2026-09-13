import { expect, it } from "bun:test";
import { Option, Schema } from "effect";
import { StreamId } from "../schema/index.ts";
import * as StreamRef from "./ref.ts";
import * as StreamRoute from "./route.ts";

const Entry = Schema.Struct({ text: Schema.String });

it("a template route parses and builds the same id", () => {
  const journal = StreamRoute.json("journal/:user", {
    params: { user: Schema.String },
    schema: Entry,
  });
  expect(journal.template).toBe("journal/:user");
  const ref = journal.ref({ user: "ann" });
  expect(ref.id).toBe(StreamId.make("journal/ann"));
  expect(ref.contentType).toBe("application/json");
  expect(Option.getOrUndefined(journal.parse("journal/ann"))).toEqual({ user: "ann" });
  for (const id of ["journal/ann", "journal", "journal/ann/notes", "drafts/ann", "journal/"])
    expect(journal.match(id)).toBe(id === "journal/ann");
});

it("multi-segment templates keep parameter identity", () => {
  const log = StreamRoute.json("fold/sessions/:session/events", {
    params: { session: Schema.String },
    schema: Entry,
  });
  expect(Option.getOrUndefined(log.parse("fold/sessions/s1/events"))).toEqual({ session: "s1" });
  expect(log.ref({ session: "s1" }).id).toBe(StreamId.make("fold/sessions/s1/events"));
  const two = StreamRoute.json("fold/sessions/:session/:kind", {
    params: { session: Schema.String, kind: Schema.String },
    schema: Entry,
  });
  expect(Option.getOrUndefined(two.parse("fold/sessions/s1/events"))).toEqual({
    session: "s1",
    kind: "events",
  });
});

it("a parameter that fails its codec is not a match", () => {
  const order = StreamRoute.json("orders/:orderId", {
    params: { orderId: Schema.NumberFromString.pipe(Schema.check(Schema.isGreaterThan(0))) },
    schema: Entry,
  });
  expect(Option.getOrUndefined(order.parse("orders/7"))).toEqual({ orderId: 7 });
  expect(Option.isNone(order.parse("orders/0"))).toBe(true);
  expect(Option.isNone(order.parse("orders/seven"))).toBe(true);
  expect(order.match("orders/seven")).toBe(false);
  expect(order.ref({ orderId: 7 }).id).toBe(StreamId.make("orders/7"));
});

it("a built parameter must be a canonical id segment", () => {
  const upload = StreamRoute.bytes("uploads/:name", { params: { name: Schema.String } });
  expect(upload.ref({ name: "a.png" }).id).toBe(StreamId.make("uploads/a.png"));
  for (const name of ["", ".", "..", "a/b", "a%2Fb", "a?b", "a#b", "a\\b"])
    expect(() => upload.ref({ name })).toThrow(RangeError);
});

it("bytes sets the content type and defaults to octet-stream", () => {
  const upload = StreamRoute.bytes("uploads/:name", {
    params: { name: Schema.String },
    contentType: "image/png",
  });
  expect(upload.ref({ name: "a.png" }).contentType).toBe("image/png");
  expect(upload.ref({ name: "a.png" })._tag).toBe("Bytes");
  const plain = StreamRoute.bytes("blobs/:name", { params: { name: Schema.String } });
  expect(plain.ref({ name: "a" }).contentType).toBe("application/octet-stream");
});

it("a custom route wraps a grammar a template cannot express", () => {
  const sessionLog = StreamRoute.custom({
    parse: (id) => {
      const match = /^fold\/sessions\/(.+)\/events$/.exec(id);
      return match?.[1] === undefined ? Option.none() : Option.some({ session: match[1] });
    },
    ref: ({ session }) => StreamRef.json(`fold/sessions/${session}/events`, { schema: Entry }),
  });
  expect(sessionLog.template).toBe("");
  expect(Option.getOrUndefined(sessionLog.parse("fold/sessions/s1/events"))).toEqual({
    session: "s1",
  });
  expect(sessionLog.match("fold/sessions/s1/events")).toBe(true);
  expect(sessionLog.match("fold/sessions/s1")).toBe(false);
  expect(sessionLog.ref({ session: "s1" }).id).toBe(StreamId.make("fold/sessions/s1/events"));
});

it("the template constructor rejects a malformed grammar", () => {
  for (const template of ["", "/journal/:user", "journal//:user", "journal/:", "journal/:a:1"])
    expect(() =>
      StreamRoute.json(template, {
        params: { user: Schema.String, "a:1": Schema.String },
        schema: Entry,
      }),
    ).toThrow();
  expect(() =>
    StreamRoute.json("journal/:a/:a", { params: { a: Schema.String }, schema: Entry }),
  ).toThrow(RangeError);
});

it("the codec record backstop covers a widened template", () => {
  // A widened string defeats the compile-time parameter check, so the runtime
  // backstop is the only thing that catches a mismatch here.
  const widenedTemplate: string = "journal/:user";
  expect(() =>
    StreamRoute.json(widenedTemplate, { params: { user: Schema.String }, schema: Entry }),
  ).not.toThrow();
  expect(() =>
    StreamRoute.json(widenedTemplate, {
      params: { user: Schema.String, extra: Schema.String },
      schema: Entry,
    }),
  ).toThrow(RangeError);
  expect(() => StreamRoute.json(widenedTemplate, { params: {}, schema: Entry })).toThrow(
    RangeError,
  );
});
