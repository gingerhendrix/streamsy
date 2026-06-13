import type {
  AppendOptions,
  AppendResult,
  CreateOptions,
  CreateResult,
  DeleteResult,
  MetadataResult,
  NotSupportedResult,
  ProtocolGetResult,
  ProtocolStream,
  ReadLiveOptions,
  ReadLiveResult,
  ReadOptions,
  ReadResult,
  StoredMessage,
  StreamId,
  StreamProtocolFactory,
} from "@streamsy/core";

export const JSON_CONTENT_TYPE = "application/json";

type StandardSchemaResult<T> =
  | { value: T; issues?: undefined }
  | { value?: undefined; issues: readonly unknown[] };

type StandardSchema<T> = {
  "~standard": {
    validate(value: unknown): StandardSchemaResult<T> | Promise<StandardSchemaResult<T>>;
  };
};

export interface JsonCodec<T> {
  encode(value: T): unknown;
  decode(value: unknown): T;
}

export type JsonSchema<T> = JsonCodec<T> | StandardSchema<T>;

export class JsonValidationError extends Error {
  constructor(
    message: string,
    public override readonly cause?: unknown,
  ) {
    super(message);
    this.name = "JsonValidationError";
  }
}

export type JsonStoredMessage<T> = StoredMessage & { value: T };

export type JsonCreateOptions<T> = Omit<CreateOptions, "contentType" | "initialData"> & {
  initialMessage?: T;
  initialMessages?: readonly T[];
};

/**
 * Append options minus the byte-level fields. `expectedOffset` (the CAS
 * precondition) is per-append: it is not meaningful with `appendMany`, whose
 * appends run concurrently — a shared `expectedOffset` would fail all but one.
 */
export type JsonAppendOptions = Omit<AppendOptions, "data" | "contentType">;

export type JsonCreateResult<T> =
  | (Omit<Extract<CreateResult, { status: "created" }>, "stream"> & { stream: JsonStream<T> })
  | (Omit<Extract<CreateResult, { status: "exists" }>, "stream"> & { stream: JsonStream<T> })
  | Exclude<CreateResult, { status: "created" } | { status: "exists" }>;

export type JsonGetResult<T> =
  | { status: "ok"; stream: JsonStream<T> }
  | { status: "content-type-conflict"; contentType: string; expectedContentType: string }
  | Exclude<ProtocolGetResult, { status: "ok" }>;

export type JsonReadResult<T> =
  | (Omit<Extract<ReadResult, { status: "ok" }>, "messages"> & { messages: JsonStoredMessage<T>[] })
  | Exclude<ReadResult, { status: "ok" }>
  | { status: "invalid-json"; error: unknown; offset?: string };

// ReadLiveResult models all data-carrying statuses ("ok" | "timeout" |
// "not-found" | "gone") as one member, so the typed variant swaps its
// messages wholesale rather than extracting by status.
export type JsonReadLiveResult<T> =
  | (Omit<Extract<ReadLiveResult, { messages: StoredMessage[] }>, "messages"> & {
      messages: JsonStoredMessage<T>[];
    })
  | Exclude<ReadLiveResult, { messages: StoredMessage[] }>
  | { status: "invalid-json"; error: unknown; offset?: string };

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function normalizeJsonCodec<T>(schema: JsonSchema<T>): JsonCodec<T> {
  if ("decode" in schema && typeof schema.decode === "function") {
    return schema;
  }
  if ("~standard" in schema) {
    return {
      encode(value) {
        return value;
      },
      decode(value) {
        const result = schema["~standard"].validate(value);
        if (result instanceof Promise) {
          throw new JsonValidationError("Async Standard Schema validation is not supported");
        }
        if ("issues" in result && result.issues) {
          throw new JsonValidationError("JSON value failed schema validation", result.issues);
        }
        return result.value;
      },
    };
  }
  throw new JsonValidationError("Unsupported JSON schema");
}

function isJsonContentType(contentType: string): boolean {
  return contentType.toLowerCase().split(";", 1)[0]?.trim() === JSON_CONTENT_TYPE;
}

function encodeJson<T>(codec: JsonCodec<T>, value: T): Uint8Array {
  return encoder.encode(JSON.stringify(codec.encode(value)));
}

function decodeJsonMessage<T>(codec: JsonCodec<T>, message: StoredMessage): JsonStoredMessage<T> {
  const parsed = JSON.parse(decoder.decode(message.data)) as unknown;
  return { ...message, value: codec.decode(parsed) };
}

function decodeMessages<T>(
  codec: JsonCodec<T>,
  messages: StoredMessage[],
): { ok: true; messages: JsonStoredMessage<T>[] } | { ok: false; error: unknown; offset?: string } {
  const decoded: JsonStoredMessage<T>[] = [];
  for (const message of messages) {
    try {
      decoded.push(decodeJsonMessage(codec, message));
    } catch (error) {
      return { ok: false, error, offset: message.offset };
    }
  }
  return { ok: true, messages: decoded };
}

function notSupportedLike(
  result: ProtocolGetResult | MetadataResult,
): result is NotSupportedResult {
  return result.status === "not-supported";
}

export class JsonProtocol<T> {
  readonly protocol: StreamProtocolFactory;
  readonly contentType: string;
  private codec: JsonCodec<T>;

  constructor(
    protocol: StreamProtocolFactory,
    schema: JsonSchema<T>,
    options: { contentType?: typeof JSON_CONTENT_TYPE } = {},
  ) {
    this.protocol = protocol;
    this.codec = normalizeJsonCodec(schema);
    this.contentType = options.contentType ?? JSON_CONTENT_TYPE;
  }

  async create(streamId: string, options: JsonCreateOptions<T> = {}): Promise<JsonCreateResult<T>> {
    const { initialMessage, initialMessages, ...createOptions } = options;
    const messages = [
      ...(initialMessage === undefined ? [] : [initialMessage]),
      ...(initialMessages ?? []),
    ];
    const [first, ...rest] = messages;
    const result = await this.protocol.create(streamId, {
      ...createOptions,
      contentType: this.contentType,
      initialData: first === undefined ? undefined : encodeJson(this.codec, first),
    });
    if (result.status !== "created" && result.status !== "exists") return result;
    const stream = this.wrap(result.stream);
    if (result.status === "created") {
      for (const message of rest) {
        const appended = await stream.append(message);
        if (appended.status !== "appended" && appended.status !== "duplicate") break;
      }
    }
    return { ...result, stream };
  }

  async get(streamId: string): Promise<JsonGetResult<T>> {
    const result = await this.protocol.get(streamId);
    if (result.status !== "ok") return result;
    const metadata = await result.stream.metadata();
    if (metadata.status !== "ok") {
      if (notSupportedLike(metadata)) return metadata;
      return metadata;
    }
    if (!isJsonContentType(metadata.contentType)) {
      return {
        status: "content-type-conflict",
        contentType: metadata.contentType,
        expectedContentType: this.contentType,
      };
    }
    return { status: "ok", stream: this.wrap(result.stream) };
  }

  wrap(stream: ProtocolStream): JsonStream<T> {
    return new JsonStream(stream, this.codec, { contentType: this.contentType });
  }
}

export function createJsonProtocol<T>(
  protocol: StreamProtocolFactory,
  schema: JsonSchema<T>,
  options?: { contentType?: typeof JSON_CONTENT_TYPE },
): JsonProtocol<T> {
  return new JsonProtocol(protocol, schema, options);
}

export class JsonStream<T> {
  readonly stream: ProtocolStream;
  readonly id: StreamId;
  private codec: JsonCodec<T>;
  private contentType: string;

  constructor(stream: ProtocolStream, codec: JsonCodec<T>, options: { contentType?: string } = {}) {
    this.stream = stream;
    this.id = stream.id;
    this.codec = codec;
    this.contentType = options.contentType ?? JSON_CONTENT_TYPE;
  }

  append(message: T, options: JsonAppendOptions = {}): Promise<AppendResult> {
    return this.stream.append({
      ...options,
      data: encodeJson(this.codec, message),
      contentType: this.contentType,
    });
  }

  appendMany(messages: readonly T[], options: JsonAppendOptions = {}): Promise<AppendResult[]> {
    return Promise.all(messages.map((message) => this.append(message, options)));
  }

  appendJson(value: unknown, options: JsonAppendOptions = {}): Promise<AppendResult> {
    return this.stream.append({
      ...options,
      data: encoder.encode(JSON.stringify(value)),
      contentType: this.contentType,
    });
  }

  async read(options: ReadOptions = {}): Promise<JsonReadResult<T>> {
    const result = await this.stream.read(options);
    if (result.status !== "ok") return result;
    const decoded = decodeMessages(this.codec, result.messages);
    if (!decoded.ok)
      return { status: "invalid-json", error: decoded.error, offset: decoded.offset };
    return { ...result, messages: decoded.messages };
  }

  async readLive(options: ReadLiveOptions): Promise<JsonReadLiveResult<T>> {
    const result = await this.stream.readLive(options);
    if (result.status === "not-supported") return result;
    const decoded = decodeMessages(this.codec, result.messages);
    if (!decoded.ok)
      return { status: "invalid-json", error: decoded.error, offset: decoded.offset };
    return { ...result, messages: decoded.messages };
  }

  readRaw(options: ReadOptions = {}): Promise<ReadResult> {
    return this.stream.read(options);
  }

  metadata(): Promise<MetadataResult> {
    return this.stream.metadata();
  }

  delete(): Promise<DeleteResult> {
    return this.stream.delete();
  }
}
