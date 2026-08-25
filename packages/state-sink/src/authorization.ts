import { Context, Effect, Layer, Schema } from "effect";
export interface StateSinkAuthorizationTarget {
  readonly name: string;
  readonly auth: { readonly policy: string; readonly required: string };
}

export interface AuthorizedSinkContext {
  readonly generation: string;
  readonly subject?: string;
}

export class SinkAuthorizationDenied extends Schema.TaggedError<SinkAuthorizationDenied>()(
  "SinkAuthorizationDenied",
  { required: Schema.String },
) {}

export class SinkAuthorizationUnavailable extends Schema.TaggedError<SinkAuthorizationUnavailable>()(
  "SinkAuthorizationUnavailable",
  { detail: Schema.String },
) {}

export interface StateSinkAuthorizerService {
  readonly authorize: (input: {
    readonly request: Request;
    readonly sink: StateSinkAuthorizationTarget;
    readonly params: Readonly<Record<string, string>>;
  }) => Effect.Effect<
    AuthorizedSinkContext,
    SinkAuthorizationDenied | SinkAuthorizationUnavailable
  >;
}

export class StateSinkAuthorizer extends Context.Service<
  StateSinkAuthorizer,
  StateSinkAuthorizerService
>()("@streamsy/state-sink/StateSinkAuthorizer") {}

export const authorizerLayer = (
  authorize: StateSinkAuthorizerService["authorize"],
): Layer.Layer<StateSinkAuthorizer> => Layer.succeed(StateSinkAuthorizer, { authorize });
