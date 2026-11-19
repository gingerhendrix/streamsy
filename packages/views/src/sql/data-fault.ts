import { Schema } from "effect";

export const rowKeySchema = Schema.Union([
  Schema.String,
  Schema.Finite,
  Schema.Boolean,
  Schema.Array(Schema.Union([Schema.String, Schema.Finite, Schema.Boolean])),
]);

interface Location {
  readonly view: string;
  readonly node: string;
}

export class ViewDataFault extends Schema.TaggedError<
  ViewDataFault,
  { readonly brand: unique symbol }
>()("ViewDataFault", {
  view: Schema.String,
  node: Schema.String,
  key: Schema.NullOr(rowKeySchema),
  reason: Schema.String,
}) {
  override get message() {
    return `${this.view}/${this.node}: ${this.reason}`;
  }
}

export type DataFault = (key: ViewDataFault["key"], reason: string) => ViewDataFault;

export const dataFault =
  (location: Location): DataFault =>
  (key, reason) =>
    new ViewDataFault({
      ...location,
      key: Schema.is(rowKeySchema)(key) || key === null ? key : String(key),
      reason,
    });

/** Evaluate data at its row boundary. Other defects remain engine faults. */
export function checkData<A>(fault: DataFault, key: ViewDataFault["key"], evaluate: () => A): A {
  try {
    return evaluate();
  } catch (cause) {
    if (cause instanceof TypeError) throw fault(key, cause.message);
    throw cause;
  }
}
