import { HttpClientRequest } from "effect/unstable/http";
import type {
  AppendOptions,
  CreateOptions,
  ReadOptions,
  ReadNextOptions,
} from "../protocol/options.ts";
import { format } from "./wire.ts";

export interface Options {
  readonly baseUrl: string;
  readonly headers?: Readonly<Record<string, string>>;
  /** Explicit deployment contract, never inferred from a successful append. */
  readonly capabilities?: { readonly expectedOffset?: boolean; readonly producer?: boolean };
}

interface ReadQuery {
  offset: string;
  batch_size?: string;
}
interface NextQuery {
  offset: string;
  live: string;
  cursor?: string;
}

export function requests(options: Options) {
  const base = new URL(options.baseUrl);
  if (
    !/^https?:$/.test(base.protocol) ||
    base.search ||
    base.hash ||
    base.username ||
    base.password
  )
    throw new TypeError("baseUrl must be an HTTP(S) URL without credentials, query or fragment");
  const prefix = base.pathname.replace(/\/$/, "") + "/";
  const url = (id: string) => {
    // IDs are canonical HTTP path identities: reject ambiguous URL normalization.
    if (
      !id ||
      id.split("/").some((part) => !part || part === "." || part === "..") ||
      /[%?#\\]/.test(id)
    )
      throw new TypeError("Stream ID must be a canonical relative path");
    const target = new URL(base);
    target.pathname = prefix + id;
    if (target.pathname !== prefix + id) throw new TypeError("Stream ID must be URL-safe");
    return target.href;
  };
  const make = (
    method: "HEAD" | "GET" | "PUT" | "POST" | "DELETE",
    id: string,
    headers: Record<string, string> = {},
  ) =>
    HttpClientRequest.make(method)(url(id), {
      headers: { ...options.headers, ...headers, accept: format, "cache-control": "no-cache" },
    });
  return {
    head: (id: string) => make("HEAD", id),
    remove: (id: string) => make("DELETE", id),
    read: (id: string, input: ReadOptions = {}) => {
      const params: ReadQuery = { offset: input.offset ?? "-1" };
      if (input.limit !== undefined) params.batch_size = String(input.limit);
      return HttpClientRequest.setUrlParams(make("GET", id), params);
    },
    readNext: (id: string, input: ReadNextOptions) => {
      const params: NextQuery = {
        offset: input.offset,
        live: "long-poll",
      };
      if (input.cursor !== undefined) params.cursor = input.cursor;
      return HttpClientRequest.setUrlParams(make("GET", id), params);
    },
    create: (id: string, input: CreateOptions = {}) => {
      const headers: Record<string, string> = {};
      if (input.contentType !== undefined) headers["content-type"] = input.contentType;
      if (input.ttlSeconds !== undefined) headers["stream-ttl"] = String(input.ttlSeconds);
      if (input.expiresAt !== undefined) headers["stream-expires-at"] = input.expiresAt;
      if (input.closed) headers["stream-closed"] = "true";
      if (input.forkedFrom !== undefined)
        headers["stream-forked-from"] = new URL(url(input.forkedFrom)).pathname;
      if (input.forkOffset !== undefined) headers["stream-fork-offset"] = input.forkOffset;
      if (input.forkSubOffset !== undefined)
        headers["stream-fork-sub-offset"] = String(input.forkSubOffset);
      const request = make("PUT", id, headers);
      return input.initialData === undefined
        ? request
        : HttpClientRequest.bodyUint8Array(request, input.initialData, input.contentType);
    },
    append: (id: string, input: AppendOptions) => {
      const headers: Record<string, string> = {};
      if (input.seq !== undefined) headers["stream-seq"] = input.seq;
      if (input.expectedOffset !== undefined)
        headers["stream-expected-offset"] = input.expectedOffset;
      if (input.close) headers["stream-closed"] = "true";
      if (input.producer) {
        headers["producer-id"] = input.producer.producerId;
        headers["producer-epoch"] = String(input.producer.producerEpoch);
        headers["producer-seq"] = String(input.producer.producerSeq);
      }
      return HttpClientRequest.bodyUint8Array(
        make("POST", id, headers),
        input.data,
        input.contentType,
      );
    },
  };
}
