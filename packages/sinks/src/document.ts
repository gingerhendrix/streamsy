/**
 * The checked document-sink contract.
 *
 * A document sink publishes one derived value per route, not a keyed
 * collection: a workspace summary, a report, a manifest. It has no resume
 * position, because there is nothing to resume — a consumer either holds the
 * current document or it does not. What it has instead is a validator and a
 * cache policy, and both are declared rather than left to a host.
 *
 * The declaration names the relations the document is derived from, so a
 * consumer can see what invalidates it, and it declares its own decode, so a
 * document that no longer matches its schema is a typed failure instead of a
 * cached wrong answer.
 */
import {
  compileSinkRoute,
  type DecodedSinkParams,
  type SinkParamCodecs,
} from "@streamsy/state-sink";
import type { DocumentSinkErrorTag } from "./errors.ts";
import { contractFingerprint } from "./fingerprint.ts";
import type { ExactRouteParams } from "./route-params.ts";

export interface DocumentSinkCodec<Document> {
  /* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This decoder is the document's external wire boundary. */
  readonly decode: (value: unknown) => Document;
}

/** A relation a document is derived from. Naming it records what invalidates the document. */
export interface DocumentSinkSource {
  readonly name: string;
}

/**
 * The declared cache policy, lowered to one `cache-control` header.
 *
 * `maxAgeSeconds: 0` with `mustRevalidate` is the honest default for a document
 * derived from a live relation: a consumer may keep the response, but it must
 * revalidate against the entity tag before trusting it again.
 */
export interface DocumentCachePolicy {
  readonly visibility: "private" | "public";
  readonly maxAgeSeconds: number;
  readonly mustRevalidate: boolean;
}

export interface DocumentSinkSpec<Document, Params extends SinkParamCodecs> {
  readonly name: string;
  readonly from: readonly DocumentSinkSource[];
  readonly document: DocumentSinkCodec<Document>;
  readonly route: string;
  readonly params: Params;
  readonly cache: DocumentCachePolicy;
  readonly errors?: readonly DocumentSinkErrorTag[];
}

export interface CheckedDocumentSink<
  Document,
  Params extends SinkParamCodecs,
> extends DocumentSinkSpec<Document, Params> {
  readonly kind: "checked-document-sink";
  /** The declared policy as the exact `cache-control` header value it lowers to. */
  readonly cacheControl: string;
  readonly fingerprint: string;
  readonly compiledRoute: ReturnType<typeof compileSinkRoute<Params>>;
}

export function defineDocumentSink<
  Document,
  const Route extends string,
  const Params extends SinkParamCodecs,
>(
  spec: DocumentSinkSpec<Document, Params> & { readonly route: Route } & ExactRouteParams<
      Route,
      Params
    >,
): CheckedDocumentSink<Document, Params> {
  const compiledRoute = compileSinkRoute(spec.route, spec.params);
  const cacheControl = cacheControlOf(spec.cache);
  const fingerprint = contractFingerprint({
    name: spec.name,
    sources: spec.from.map((source) => source.name),
    route: spec.route,
    params: [...compiledRoute.parameterNames],
    cacheControl,
    errors: [...(spec.errors ?? [])],
  });
  return Object.freeze({
    ...spec,
    kind: "checked-document-sink",
    cacheControl,
    fingerprint,
    compiledRoute,
  });
}

/** Lower the declared policy to its header. Directive order is fixed, so it is comparable. */
export function cacheControlOf(policy: DocumentCachePolicy): string {
  const directives = [policy.visibility, `max-age=${policy.maxAgeSeconds}`];
  if (policy.mustRevalidate) directives.push("must-revalidate");
  return directives.join(", ");
}

export type DocumentOf<Sink> =
  Sink extends CheckedDocumentSink<infer Document, infer _Params> ? Document : never;
export type DocumentParamsOf<Sink> =
  Sink extends CheckedDocumentSink<infer _Document, infer Params>
    ? DecodedSinkParams<Params>
    : never;
