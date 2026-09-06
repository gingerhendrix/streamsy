/* oxlint-disable anti-slop/no-unknown-parameters -- The fixture decoder intentionally models an external row boundary. */
import { defineStateSink, type KeyOf, type ParamsOf, type RowOf } from "./state.ts";

const checked = defineStateSink({
  name: "type.fixture",
  from: { key: "id" },
  row: { decode: (_value: unknown) => ({ id: "id", label: "label" }) },
  route: "/state/:workspaceId/rows",
  params: { workspaceId: { decode: (value: string) => value } },
  collection: { name: "rows", type: "row" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
});

const row: RowOf<typeof checked> = { id: "id", label: "label" };
const key: KeyOf<typeof checked> = row.id;
const params: ParamsOf<typeof checked> = { workspaceId: "main" };
void key;
void params;

// @ts-expect-error the generated parameter set is exact
const badParams: ParamsOf<typeof checked> = { workspace: "main" };
// @ts-expect-error the generated key is a string, not a number
const badKey: KeyOf<typeof checked> = 1;
void badParams;
void badKey;

// @ts-expect-error the checked route and parameter codec names must match exactly
defineStateSink({
  name: "type.fixture.route",
  from: { key: "id" },
  row: { decode: (_value: unknown) => ({ id: "id", label: "label" }) },
  route: "/state/:workspaceId/rows",
  params: { workspace: { decode: (value: string) => value } },
  collection: { name: "rows", type: "row" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
});

defineStateSink({
  name: "type.fixture.key",
  // @ts-expect-error the relation must declare a key that is a field of the sink's row
  from: { key: "missing" },
  row: { decode: (_value: unknown) => ({ id: "id", label: "label" }) },
  route: "/state/:workspaceId/rows",
  params: { workspaceId: { decode: (value: string) => value } },
  collection: { name: "rows", type: "row" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
});
