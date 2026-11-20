import type { Schema } from "effect";
import { fn, selectors, type TypedExpression } from "./expression.ts";
import { from, parameter, source } from "./relation.ts";

interface FixtureRow {
  readonly id: string;
  readonly count: number;
  readonly enabled: boolean;
  readonly assigneeId?: string;
}

const x = selectors<FixtureRow>();

const booleanExpression: TypedExpression<boolean> = x.row.enabled.eq(true);

const optionalValue: TypedExpression<string> = x.row.assigneeId.unwrap();

void booleanExpression;

void optionalValue;

// @ts-expect-error unknown row fields must fail declaration typechecking
void x.row.missing;

// @ts-expect-error incompatible comparisons must not be lifted
x.row.count.eq("one");

// @ts-expect-error required fields do not expose optional-value unwrapping
void x.row.id.unwrap();

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

const nested = selectors<{ readonly alias?: { readonly value: { readonly label?: string } } }>();

const requiredLabel = fn("required-label", 1, (label: string) => label.toUpperCase());

const optionalLabel = fn("optional-label", 1, (label: string | undefined) => label ?? "missing");

const optionalField = nested.row.alias.value.label;

optionalLabel(optionalField);

optionalLabel(optionalField.orElse("other"));

requiredLabel(optionalField.unwrap());

// @ts-expect-error a potentially missing argument requires an explicit fallback or optional callback argument
requiredLabel(optionalField);

// @ts-expect-error promises are outside the synchronous expression contract
fn("async-label", 1, (label: string) => Promise.resolve(label));
