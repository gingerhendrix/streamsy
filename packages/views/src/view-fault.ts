import { NonRetryable } from "@streamsy/projection";
import { Schema } from "effect";

export class ViewFault extends Schema.TaggedError<ViewFault, { readonly brand: unique symbol }>()(
  "ViewFault",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export const nonRetryableViewFault = (message: string) =>
  Object.assign(new ViewFault({ message }), { [NonRetryable]: true });
