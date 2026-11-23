import { Schema } from "effect";

export class ViewFault extends Schema.TaggedError<ViewFault, { readonly brand: unique symbol }>()(
  "ViewFault",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}
