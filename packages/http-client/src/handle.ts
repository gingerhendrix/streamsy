import type { DurableStream } from "@durable-streams/client";
import type {
  AppendStreamOptions,
  AppendJsonBatchOptions,
  ClientAppendResult,
  ClientCloseResult,
  ClientCreateResult,
  ClientHeadResult,
  ClientReadResult,
  ClientRequestOptions,
  CloseStreamOptions,
  CreateStreamOptions,
  JsonValue,
  ReadStreamOptions,
  StreamProtocolHandle,
} from "@streamsy/core";
import type { OfficialProtocolClient } from "./client.ts";
import {
  appendErrorResult,
  closeErrorResult,
  createErrorResult,
  headErrorResult,
} from "./errors.ts";
import { officialAppend } from "./append.ts";
import { officialRead } from "./read.ts";

/** The official handle constructs `DurableStream` operations over one endpoint URL. */
export class OfficialProtocolHandle implements StreamProtocolHandle {
  readonly id: string;
  readonly url: string | URL;
  private readonly handle: DurableStream;

  constructor(
    readonly client: OfficialProtocolClient,
    streamId: string,
    url: string | URL,
  ) {
    this.id = streamId;
    this.url = url;
    this.handle = client.durableStream(url, client.signal);
  }

  async head(options?: ClientRequestOptions): Promise<ClientHeadResult> {
    return this.client.run<ClientHeadResult>(
      options?.signal,
      async (signal) => {
        const result = await this.handle.head({ signal });
        if (!result.exists) return { status: "not-found" };
        return {
          status: "ok",
          contentType: result.contentType,
          offset: result.offset,
          closed: result.streamClosed,
        };
      },
      headErrorResult,
    );
  }

  async create(options: CreateStreamOptions = {}): Promise<ClientCreateResult> {
    return this.client.run<ClientCreateResult>(
      options.signal,
      async (signal) => {
        // Compat: create() takes no per-call signal, so use a fresh handle.
        const handle = this.client.durableStream(this.url, signal);
        await handle.create({
          contentType: options.contentType,
          ttlSeconds: options.ttlSeconds,
          expiresAt: options.expiresAt,
          body: options.initialData,
          closed: options.closed,
        });
        return { status: "created", contentType: handle.contentType ?? options.contentType };
      },
      createErrorResult,
    );
  }

  async append(
    data: Uint8Array | string,
    options: AppendStreamOptions = {},
  ): Promise<ClientAppendResult> {
    return this.client.run<ClientAppendResult>(
      options.signal,
      (signal) =>
        officialAppend(
          this.client,
          this.url,
          data,
          { ...options, signal },
          this.handle.contentType,
        ),
      appendErrorResult,
    );
  }

  async appendJsonBatch(
    items: readonly JsonValue[],
    options: AppendJsonBatchOptions = {},
  ): Promise<ClientAppendResult> {
    if (items.length === 0) throw new TypeError("appendJsonBatch requires at least one item");
    return this.client.run<ClientAppendResult>(
      options.signal,
      (signal) =>
        officialAppend(
          this.client,
          this.url,
          JSON.stringify(items),
          { ...options, contentType: "application/json", signal },
          undefined,
          true,
        ),
      appendErrorResult,
    );
  }

  async close(options: CloseStreamOptions = {}): Promise<ClientCloseResult> {
    return this.client.run<ClientCloseResult>(
      options.signal,
      async (signal) => {
        const result = await this.handle.close({
          body: options.finalData,
          contentType: options.contentType,
          signal,
        });
        return { status: "closed", finalOffset: result.finalOffset };
      },
      closeErrorResult,
    );
  }

  async read<T extends JsonValue = JsonValue>(
    options: ReadStreamOptions = {},
  ): Promise<ClientReadResult<T>> {
    return officialRead<T>(this.client, this.url, options);
  }
}
