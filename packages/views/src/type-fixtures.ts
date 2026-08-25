import type { Schema } from "effect";
import { from, parameter, selectors, source, type TypedExpression } from "./index.ts";

interface FixtureRow {
  readonly id: string;
  readonly count: number;
  readonly enabled: boolean;
  readonly assigneeId?: string;
}

const x = selectors<FixtureRow>();
const booleanExpression: TypedExpression<boolean> = x.row.enabled.eq(true);
const optionalValue: TypedExpression<string> = x.row.assigneeId.value;
void booleanExpression;
void optionalValue;

// @ts-expect-error unknown row fields must fail declaration typechecking
void x.row.missing;
// @ts-expect-error incompatible comparisons must not be lifted
x.row.count.eq("one");
// @ts-expect-error required fields do not expose optional-value unwrapping
void x.row.id.value;
// @ts-expect-error optional values must be unwrapped or defaulted before ordering
x.row.assigneeId.asc();

declare const fixtureSchema: Schema.Codec<FixtureRow>;
const fixtureSource = source("fixture", {
  schema: fixtureSchema,
  schemaRef: { name: "FixtureRow", version: 1 },
  partitionBy: x.row.id,
  key: "id",
  mode: "facts",
});
// @ts-expect-error filters require boolean expressions
from(fixtureSource).where(x.row.count);

parameter("limit", fixtureSchema, { maximum: 10 });
