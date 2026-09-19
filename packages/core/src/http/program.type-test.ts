/**
 * The `HttpEffect` shape contract for the public HTTP program.
 *
 * A host types the program against an Alchemy `HttpEffect`, so three facts must
 * hold: the success type is `HttpServerResponse` alone, the error channel is
 * closed, and the requirement set is exactly the request plus the two protocol
 * services. `bun test` does not collect this file, so only `tsc` reads it and
 * every assertion below is checked at compile time.
 */
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { app } from "./index.ts";

type Actual = ReturnType<typeof app>;

type Success = Effect.Success<Actual>;
type Failure = Effect.Error<Actual>;
type Requirements = Effect.Services<Actual>;

/** Success is narrowed to `HttpServerResponse`; raw Web responses are wrapped by `app`. */
const response: Success = HttpServerResponse.empty({ status: 200 });
void response;

// @ts-expect-error the raw Web response is not part of the success type
const raw: Success = new Response("x");
void raw;

// @ts-expect-error the error channel is closed, so no failure type is assignable to it
const failure: Failure = new Error("closed");
void failure;

type Contains<Set, Member> = [Member] extends [Set] ? true : false;
type Exactly<Set, All> = [All] extends [Set] ? ([Set] extends [All] ? true : false) : false;

const requiresRequest: Contains<Requirements, HttpServerRequest.HttpServerRequest> = true;
const requiresReader: Contains<Requirements, StreamsReader> = true;
const requiresWriter: Contains<Requirements, StreamsWriter> = true;
const exact: Exactly<
  Requirements,
  HttpServerRequest.HttpServerRequest | StreamsReader | StreamsWriter
> = true;
void requiresRequest;
void requiresReader;
void requiresWriter;
void exact;

// @ts-expect-error the requirement set excludes anything that is not the request or the two services
const extra: Exactly<Requirements, StreamsReader | StreamsWriter | URL> = true;
void extra;
