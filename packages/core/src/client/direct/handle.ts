import type { MetadataResult, ProtocolStream } from "../../types/protocol.ts";
import type {
  AppendStreamOptions,
  ClientAppendResult,
  ClientCloseResult,
  ClientCreateResult,
  ClientFailure,
  ClientHeadResult,
  ClientReadResult,
  ClientRequestOptions,
  CloseStreamOptions,
  CreateStreamOptions,
  JsonValue,
  ReadStreamOptions,
  StreamProtocolHandle,
} from "../types.ts";
import type { DirectProtocolClient } from "./client.ts";
import { directRead } from "./read.ts";
import { absentResult, failure, mapAppend, mapClose, mapCreate } from "./results.ts";

const encoder = new TextEncoder();

type Absent = { status: "not-found" } | { status: "gone" } | ClientFailure;
type Resolved = { ok: true; stream: ProtocolStream } | { ok: false; result: Absent };

/** The direct handle is a thin pass-through over a `StreamProtocolFactory`. */
export class DirectProtocolHandle implements StreamProtocolHandle {
  readonly id: string;

  constructor(
    private readonly client: DirectProtocolClient,
    streamId: string,
  ) {
    this.id = streamId;
  }

  async head(options?: ClientRequestOptions): Promise<ClientHeadResult> {
    return this.client.run(options?.signal, async () => {
      const found = await this.client.streamsy.get(this.id);
      if (found.status !== "ok") return absentResult(found);
      const metadata = await found.stream.metadata();
      if (metadata.status !== "ok") return absentResult(metadata);
      return {
        status: "ok",
        contentType: metadata.contentType,
        offset: metadata.nextOffset,
        closed: metadata.closed === true,
      };
    });
  }

  async create(options: CreateStreamOptions = {}): Promise<ClientCreateResult> {
    return this.client.run(options.signal, async () => {
      const result = await this.client.streamsy.create(this.id, {
        contentType: options.contentType,
        ttlSeconds: options.ttlSeconds,
        expiresAt: options.expiresAt,
        initialData: toBytes(options.initialData),
        closed: options.closed,
      });
      return mapCreate(result);
    });
  }

  async append(
    data: Uint8Array | string,
    options: AppendStreamOptions = {},
  ): Promise<ClientAppendResult> {
    return this.client.run(options.signal, async () => {
      if (options.contentType === undefined) {
        return failure("bad-request", "append requires a contentType");
      }
      const resolved = await this.resolve();
      if (!resolved.ok) return resolved.result;
      const result = await resolved.stream.append({
        data: toBytes(data)!,
        contentType: options.contentType,
        seq: options.seq,
      });
      return mapAppend(result);
    });
  }

  async close(options: CloseStreamOptions = {}): Promise<ClientCloseResult> {
    return this.client.run(options.signal, async () => {
      const resolved = await this.resolve();
      if (!resolved.ok) return resolved.result;
      let contentType = options.contentType;
      if (contentType === undefined) {
        const found = await this.contentTypeFor(resolved.stream);
        if (!found.ok) return found.result;
        contentType = found.contentType;
      }
      const result = await resolved.stream.append({
        data: toBytes(options.finalData) ?? new Uint8Array(),
        contentType,
        close: true,
      });
      return mapClose(result);
    });
  }

  async read<T extends JsonValue = JsonValue>(
    options: ReadStreamOptions = {},
  ): Promise<ClientReadResult<T>> {
    return directRead<T>(this.client, this.id, options);
  }

  private async resolve(): Promise<Resolved> {
    const found = await this.client.streamsy.get(this.id);
    if (found.status === "ok") return { ok: true, stream: found.stream };
    return { ok: false, result: absentResult(found) };
  }

  private async contentTypeFor(
    stream: ProtocolStream,
  ): Promise<{ ok: true; contentType: string } | { ok: false; result: Absent }> {
    const metadata: MetadataResult = await stream.metadata();
    if (metadata.status === "ok") return { ok: true, contentType: metadata.contentType };
    return { ok: false, result: absentResult(metadata) };
  }
}

function toBytes(value: Uint8Array | string | undefined): Uint8Array | undefined {
  return typeof value === "string" ? encoder.encode(value) : value;
}
