import { Schema } from "effect";

/** A folded row disagreed with the key that was used to load its previous row. */
export class FoldByKeyError extends Schema.TaggedError<
  FoldByKeyError,
  { readonly brand: unique symbol }
>()("FoldByKeyError", {
  message: Schema.String,
}) {}
