import { describe, expect, test } from "bun:test";
import { invalidBody, readMutation } from "./utils.ts";
import { errorResponseSchema, issueInput, txIdSchema } from "../shared/state-schema.ts";

function post(body: string): Request {
  return new Request("http://localhost/api/w/main/issues", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
}

describe("readMutation", () => {
  test("accepts a JSON object body without a txid", async () => {
    const mutation = await readMutation(post(`{"title":"Ship it"}`));
    expect(mutation).not.toBeInstanceOf(Response);
    if (mutation instanceof Response) return;
    expect(mutation.body).toEqual({ title: "Ship it" });
    expect(mutation.txid).toBeUndefined();
  });

  test("keeps a well-formed client txid", async () => {
    const txid = crypto.randomUUID();
    const mutation = await readMutation(post(JSON.stringify({ txid })));
    expect(mutation).not.toBeInstanceOf(Response);
    if (mutation instanceof Response) return;
    expect(mutation.txid).toBe(txid);
  });

  test("answers 400 for invalid JSON", async () => {
    const mutation = await readMutation(post("not json"));
    expect(mutation).toBeInstanceOf(Response);
    if (!(mutation instanceof Response)) return;
    expect(mutation.status).toBe(400);
    expect(await mutation.json()).toEqual({ error: "Invalid JSON body" });
  });

  test.each([`[]`, `"text"`, `7`, `null`])(
    "answers 400 for the non-object body %s",
    async (body) => {
      const mutation = await readMutation(post(body));
      expect(mutation).toBeInstanceOf(Response);
      if (!(mutation instanceof Response)) return;
      expect(mutation.status).toBe(400);
      expect(await mutation.json()).toEqual({ error: "Body must be a JSON object" });
    },
  );

  test("answers 400 for a txid that is not a transaction id", async () => {
    const mutation = await readMutation(post(`{"txid":"nope"}`));
    expect(mutation).toBeInstanceOf(Response);
    if (!(mutation instanceof Response)) return;
    expect(mutation.status).toBe(400);
    expect(await mutation.json()).toEqual({ error: "Invalid txid" });
  });
});

describe("txIdSchema", () => {
  test("accepts a uuid", () => {
    expect(txIdSchema.safeParse(crypto.randomUUID()).success).toBe(true);
  });

  test.each([["nope"], [""], [42], [undefined], [null], [{}]])("rejects %p", (value) => {
    expect(txIdSchema.safeParse(value).success).toBe(false);
  });
});

describe("invalidBody", () => {
  test("names the offending field", async () => {
    const parsed = issueInput.safeParse({ title: 42 });
    expect(parsed.success).toBe(false);
    if (parsed.success) return;

    const response = invalidBody("issue", parsed.error);
    expect(response.status).toBe(400);
    const payload = errorResponseSchema.parse(await response.json());
    expect(payload.error).toContain("title");
  });
});
