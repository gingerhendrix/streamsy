/**
 * Serving one checked document sink.
 *
 * The handler builds the document through a host capability, validates it with
 * the declared decode, and serves its canonical encoding. The entity tag is
 * computed from those exact bytes, so it is deterministic: the same document
 * always produces the same validator, on any host, in any process.
 *
 * A conditional request is answered from the same computation. There is no
 * separate "is it still current" path that could disagree with the body path.
 */
import { Effect, Schema } from "effect";
import type { DecodedSinkParams, SinkParamCodecs } from "../route.ts";
import {
  type CheckedDocumentSink,
  type DocumentSinkPublicError,
  DOCUMENT_SINK_CONTRACT_HEADER,
} from "../document.ts";
import { canonicalJson, documentEtag, type CanonicalValue } from "../fingerprint.ts";

export class DocumentSinkSourceFailure extends Schema.TaggedError<DocumentSinkSourceFailure>()(
  "DocumentSinkSourceFailure",
  { detail: Schema.String },
) {}

export interface DocumentSinkServerCapabilities<Params, Requirements = never> {
  /** Build the current document for these parameters, as JSON the declared schema accepts. */
  readonly document: (
    params: Params,
  ) => Effect.Effect<CanonicalValue, DocumentSinkSourceFailure, Requirements>;
}

export function handleDocumentSink<Document, Params extends SinkParamCodecs, Requirements>(
  sink: CheckedDocumentSink<Document, Params>,
  request: Request,
  capabilities: DocumentSinkServerCapabilities<DecodedSinkParams<Params>, Requirements>,
): Effect.Effect<Response, never, Requirements> {
  const matched = sink.compiledRoute.match(new URL(request.url).pathname);
  if (matched.kind === "mismatch") {
    return Effect.succeed(
      errorResponse(404, sink, {
        _tag: "InvalidSinkParams",
        sink: sink.name,
        parameter: "route",
        detail: "request path does not match the checked sink route",
      }),
    );
  }
  if (matched.kind === "invalid") {
    return Effect.succeed(
      errorResponse(400, sink, {
        _tag: "InvalidSinkParams",
        sink: sink.name,
        parameter: matched.parameter,
        detail: matched.detail,
      }),
    );
  }

  const params = matched.params;
  return Effect.gen(function* () {
    const receivedContract = request.headers.get(DOCUMENT_SINK_CONTRACT_HEADER);
    if (receivedContract !== null && receivedContract !== sink.fingerprint) {
      return errorResponse(409, sink, {
        _tag: "ContractChanged",
        sink: sink.name,
        recovery: "refetch",
      });
    }

    const value = yield* capabilities.document(params);
    yield* decodeDocument(sink, value);
    const canonical = canonicalJson(value);
    const etag = documentEtag(canonical);

    const headers = documentHeaders(sink, etag);
    if (matchesEtag(request.headers.get("if-none-match"), etag)) {
      return new Response(null, { status: 304, headers });
    }
    headers.set("content-type", "application/json");
    const body = request.method === "HEAD" ? null : canonical;
    return new Response(body, { status: 200, headers });
  }).pipe(
    Effect.catchTags({
      DocumentSinkSourceFailure: (failure: DocumentSinkSourceFailure) =>
        Effect.succeed(
          errorResponse(503, sink, {
            _tag: "DocumentUnavailable",
            sink: sink.name,
            detail: failure.detail,
          }),
        ),
      DocumentSinkDecodeFailure: (failure: DocumentSinkDecodeFailure) =>
        Effect.succeed(
          errorResponse(500, sink, {
            _tag: "WireDecodeFailed",
            sink: sink.name,
            detail: failure.detail,
          }),
        ),
    }),
  );
}

class DocumentSinkDecodeFailure extends Schema.TaggedError<DocumentSinkDecodeFailure>()(
  "DocumentSinkDecodeFailure",
  { detail: Schema.String },
) {}

const decodeDocument = <Document, Params extends SinkParamCodecs>(
  sink: CheckedDocumentSink<Document, Params>,
  value: CanonicalValue,
): Effect.Effect<Document, DocumentSinkDecodeFailure> =>
  Effect.try({
    try: () => sink.document.decode(value),
    catch: (cause) =>
      new DocumentSinkDecodeFailure({
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

/**
 * `If-None-Match` matching.
 *
 * `*` matches any current representation. Otherwise the header is a list of
 * entity tags, and a weak validator matches the same representation as its
 * strong form, which is what a proxy may have rewritten it to.
 */
function matchesEtag(header: string | null, etag: string): boolean {
  if (header === null) return false;
  if (header.trim() === "*") return true;
  return header
    .split(",")
    .map((candidate) => candidate.trim())
    .map((candidate) => (candidate.startsWith("W/") ? candidate.slice(2) : candidate))
    .includes(etag);
}

function documentHeaders<Document, Params extends SinkParamCodecs>(
  sink: CheckedDocumentSink<Document, Params>,
  etag: string,
): Headers {
  return new Headers({
    [DOCUMENT_SINK_CONTRACT_HEADER]: sink.fingerprint,
    "cache-control": sink.cacheControl,
    etag,
  });
}

function errorResponse<Document, Params extends SinkParamCodecs>(
  status: number,
  sink: CheckedDocumentSink<Document, Params>,
  error: DocumentSinkPublicError,
): Response {
  return new Response(JSON.stringify(error), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      [DOCUMENT_SINK_CONTRACT_HEADER]: sink.fingerprint,
    },
  });
}
