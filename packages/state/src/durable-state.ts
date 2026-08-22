import {
  JsonProtocol,
  JsonStream,
  normalizeJsonCodec,
  type JsonCodec,
  type JsonReadLiveResult,
  type JsonReadResult,
  type JsonSchema,
} from "@streamsy/json";
import type {
  AppendResult,
  CreateOptions,
  DeleteResult,
  MetadataResult,
  ProtocolStream,
  ReadLiveOptions,
  ReadOptions,
  StreamId,
  StreamProtocolFactory,
} from "@streamsy/core";

export type DurableStateControl = "snapshot-start" | "snapshot-end" | "reset";
export type DurableStateOperation = "insert" | "update" | "delete";
export type DurableStateOperationWithExtensions = DurableStateOperation | "upsert";

export interface DurableStateChangeHeaders {
  operation: DurableStateOperationWithExtensions;
  txid?: string;
  timestamp?: string;
  from?: string;
  offset?: string;
}

export type DurableStateUserHeaders = Omit<DurableStateChangeHeaders, "operation">;
export type DurableStateControlHeaders = { offset?: string };

export type InsertMessage<Type extends string, Value> = {
  type: Type;
  key: string;
  value: Value;
  headers: DurableStateChangeHeaders & { operation: "insert" };
};

export type UpdateMessage<Type extends string, Value> = {
  type: Type;
  key: string;
  value: Value;
  old_value?: Value;
  headers: DurableStateChangeHeaders & { operation: "update" };
};

export type DeleteMessage<Type extends string, Value> = {
  type: Type;
  key: string;
  value?: null;
  old_value?: Value;
  headers: DurableStateChangeHeaders & { operation: "delete" };
};

export type UpsertMessage<Type extends string, Value> = {
  type: Type;
  key: string;
  value: Value;
  old_value?: Value;
  headers: DurableStateChangeHeaders & { operation: "upsert" };
};

export type ChangeMessage<Type extends string, Value> =
  | InsertMessage<Type, Value>
  | UpdateMessage<Type, Value>
  | UpsertMessage<Type, Value>
  | DeleteMessage<Type, Value>;

export type ControlMessage = {
  headers: { control: DurableStateControl; offset?: string };
};

export type DurableStateMessage<RowMap extends Record<string, unknown>> =
  | { [Type in keyof RowMap & string]: ChangeMessage<Type, RowMap[Type]> }[keyof RowMap & string]
  | ControlMessage;

export interface DurableStateCollectionDef<T> {
  schema: JsonSchema<T>;
  type?: string;
  primaryKey: string | ((value: T) => string);
}

export type DurableStateSchemaMap = Record<string, DurableStateCollectionDef<unknown>>;
export type CollectionValue<Def> = Def extends DurableStateCollectionDef<infer T> ? T : never;
export type ValuesByWireType<S extends DurableStateSchemaMap> = {
  [K in keyof S as S[K]["type"] extends string ? S[K]["type"] : K & string]: CollectionValue<S[K]>;
};

export type DurableStateCreateOptions = Omit<CreateOptions, "contentType" | "initialData">;
export type DurableStateCreateResult<S extends DurableStateSchemaMap> =
  | {
      status: "created";
      stream: DurableStateStream<S>;
      nextOffset: string;
      contentType: string;
      closed?: boolean;
    }
  | {
      status: "exists";
      stream: DurableStateStream<S>;
      nextOffset: string;
      contentType: string;
      closed?: boolean;
    }
  | {
      status: "conflict";
      nextOffset: string;
      contentType: string;
      conflictReason?: string;
      errorMessage?: string;
    }
  | {
      status: "not-found" | "bad-request";
      nextOffset: string;
      contentType: string;
      errorMessage?: string;
    }
  | { status: "not-supported"; feature: string; message?: string };
export type DurableStateGetResult<S extends DurableStateSchemaMap> =
  | { status: "ok"; stream: DurableStateStream<S> }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "not-supported"; feature: string; message?: string }
  | { status: "content-type-conflict"; contentType: string; expectedContentType: string };

type CollectionRuntime = {
  key: string;
  wireType: string;
  codec: JsonCodec<unknown>;
  primaryKey: DurableStateCollectionDef<unknown>["primaryKey"];
};

const operations = new Set<string>(["insert", "update", "upsert", "delete"]);
const controls = new Set<string>(["snapshot-start", "snapshot-end", "reset"]);

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOwn(obj: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(obj, key);
}

function buildRuntime(schema: DurableStateSchemaMap): CollectionRuntime[] {
  return Object.entries(schema).map(([key, def]) => ({
    key,
    wireType: def.type ?? key,
    codec: normalizeJsonCodec(def.schema),
    primaryKey: def.primaryKey,
  }));
}

/**
 * The one checked collection-key lookup: resolves a schema-map key to its
 * runtime entry, or rejects the key.
 */
function requireByKey(runtime: CollectionRuntime[], key: string): CollectionRuntime {
  const def = runtime.find((entry) => entry.key === key);
  if (!def) throw new Error(`Unknown Durable State collection: ${key}`);
  return def;
}

/**
 * The one checked wire-tag-to-codec lookup. Every path that needs a collection
 * codec for an on-the-wire `type` tag resolves it here, so an unknown tag is
 * rejected identically whether it arrives through `append`, `encode` or
 * `decode`.
 */
function requireByWireType(runtime: CollectionRuntime[], wireType: string): CollectionRuntime {
  const def = runtime.find((entry) => entry.wireType === wireType);
  if (!def) throw new Error(`Unknown Durable State type: ${wireType}`);
  return def;
}

function keyFromValue(def: CollectionRuntime, value: unknown): string {
  if (typeof def.primaryKey === "function") return def.primaryKey(value);
  if (!isObject(value))
    throw new Error(`Cannot extract primary key ${def.primaryKey} from non-object value`);
  const key = value[def.primaryKey];
  if (typeof key !== "string") throw new Error(`Primary key ${def.primaryKey} must be a string`);
  return key;
}

/**
 * Checks `value` against the Durable State wire vocabulary and the collection
 * table, and returns the same object so encoding preserves it byte-for-byte.
 *
 * A returned change message is proven to carry a `type` that
 * {@link requireByWireType} resolved, a string `key`, a known `operation`, and a
 * `value`/`old_value` that the resolved collection codec accepted. A returned
 * control message is proven to carry a known `control` header and no `type` or
 * `key`.
 */
function validateMessage(runtime: CollectionRuntime[], value: unknown): Record<string, unknown> {
  if (!isObject(value)) throw new Error("Durable State message must be an object");
  const headers = value.headers;
  if (!isObject(headers)) throw new Error("Durable State message requires headers object");

  if ("control" in headers) {
    if (typeof headers.control !== "string" || !controls.has(headers.control)) {
      throw new Error("Invalid Durable State control message");
    }
    if ("type" in value || "key" in value) {
      throw new Error("Durable State control messages must not include type or key");
    }
    return value;
  }

  const type = value.type;
  const key = value.key;
  const operation = headers.operation;
  if (typeof type !== "string" || type.length === 0)
    throw new Error("Change message type must be a string");
  if (typeof key !== "string" || key.length === 0)
    throw new Error("Change message key must be a string");
  if (typeof operation !== "string" || !operations.has(operation)) {
    throw new Error("Invalid Durable State operation");
  }

  const def = requireByWireType(runtime, type);

  if (operation === "insert" || operation === "update" || operation === "upsert") {
    if (!hasOwn(value, "value")) throw new Error(`${operation} message requires value`);
    def.codec.decode(value.value);
  } else {
    if (hasOwn(value, "value") && value.value !== null) {
      throw new Error("delete message value must be null when present");
    }
  }
  if (hasOwn(value, "old_value")) def.codec.decode(value.old_value);
  return value;
}

/**
 * Validates `value` and narrows it to the schema-typed message union.
 *
 * The invariant this stands on: `runtime` is always produced from the same
 * schema map `S` by {@link buildRuntime}, so `wireType` ranges exactly over the
 * wire tags of `ValuesByWireType<S>` and each entry's `codec` is the codec of
 * that tag's collection. {@link validateMessage} therefore proves that an
 * accepted change message is a `ChangeMessage<Tag, ValuesByWireType<S>[Tag]>`
 * for some tag of `S`, and that an accepted control message is a
 * `ControlMessage` — together, exactly the members of
 * `DurableStateMessage<ValuesByWireType<S>>`.
 *
 * TypeScript cannot relate the value-level `CollectionRuntime[]` table to the
 * type-level map `S`, so that last step is asserted here. This is the single
 * unchecked narrowing in the package; every other path reaches the typed union
 * through this function.
 */
function toDurableStateMessage<S extends DurableStateSchemaMap>(
  runtime: CollectionRuntime[],
  value: unknown,
): DurableStateMessage<ValuesByWireType<S>> {
  const validated = validateMessage(runtime, value);
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Runtime-proven by `validateMessage`; see the invariant documented above.
  return validated as DurableStateMessage<ValuesByWireType<S>>;
}

function durableStateCodec<S extends DurableStateSchemaMap>(
  runtime: CollectionRuntime[],
): JsonCodec<DurableStateMessage<ValuesByWireType<S>>> {
  return {
    encode(value) {
      return validateMessage(runtime, value);
    },
    decode(value) {
      return toDurableStateMessage<S>(runtime, value);
    },
  };
}

export interface DurableState<S extends DurableStateSchemaMap> {
  append(message: DurableStateMessage<ValuesByWireType<S>>): Promise<AppendResult>;
  insert<K extends keyof S & string>(
    type: K,
    value: CollectionValue<S[K]>,
    options?: { key?: string; headers?: DurableStateUserHeaders },
  ): Promise<AppendResult>;
  update<K extends keyof S & string>(
    type: K,
    value: CollectionValue<S[K]>,
    options?: { key?: string; oldValue?: CollectionValue<S[K]>; headers?: DurableStateUserHeaders },
  ): Promise<AppendResult>;
  upsert<K extends keyof S & string>(
    type: K,
    value: CollectionValue<S[K]>,
    options?: { key?: string; oldValue?: CollectionValue<S[K]>; headers?: DurableStateUserHeaders },
  ): Promise<AppendResult>;
  delete<K extends keyof S & string>(
    type: K,
    key: string,
    options?: { oldValue?: CollectionValue<S[K]>; headers?: DurableStateUserHeaders },
  ): Promise<AppendResult>;
  snapshotStart(options?: {
    offset?: string;
    headers?: DurableStateControlHeaders;
  }): Promise<AppendResult>;
  snapshotEnd(options?: {
    offset?: string;
    headers?: DurableStateControlHeaders;
  }): Promise<AppendResult>;
  reset(options?: { offset?: string; headers?: DurableStateControlHeaders }): Promise<AppendResult>;
}

export class DurableStateProtocol<S extends DurableStateSchemaMap> {
  readonly protocol: StreamProtocolFactory;
  readonly json: JsonProtocol<DurableStateMessage<ValuesByWireType<S>>>;
  readonly schema: S;
  private runtime: CollectionRuntime[];

  constructor(protocol: StreamProtocolFactory, schema: S) {
    this.protocol = protocol;
    this.schema = schema;
    this.runtime = buildRuntime(schema);
    this.json = new JsonProtocol(protocol, durableStateCodec<S>(this.runtime));
  }

  async create(
    streamId: string,
    options: DurableStateCreateOptions = {},
  ): Promise<DurableStateCreateResult<S>> {
    const result = await this.json.create(streamId, options);
    if (result.status !== "created" && result.status !== "exists") return result;
    return { ...result, stream: this.wrap(result.stream.stream) };
  }

  async get(streamId: string): Promise<DurableStateGetResult<S>> {
    const result = await this.json.get(streamId);
    if (result.status !== "ok") return result;
    return { status: "ok", stream: this.wrap(result.stream.stream) };
  }

  wrap(stream: ProtocolStream): DurableStateStream<S> {
    return new DurableStateStream(this.json.wrap(stream), this.schema, this.runtime);
  }
}

export function createDurableStateProtocol<S extends DurableStateSchemaMap>(
  protocol: StreamProtocolFactory,
  schema: S,
): DurableStateProtocol<S> {
  return new DurableStateProtocol(protocol, schema);
}

export class DurableStateStream<S extends DurableStateSchemaMap> {
  readonly json: JsonStream<DurableStateMessage<ValuesByWireType<S>>>;
  readonly stream: ProtocolStream;
  readonly id: StreamId;
  readonly state: DurableState<S>;
  private runtime: CollectionRuntime[];

  constructor(
    json: JsonStream<DurableStateMessage<ValuesByWireType<S>>>,
    schema: S,
    runtime = buildRuntime(schema),
  ) {
    this.json = json;
    this.stream = json.stream;
    this.id = json.id;
    this.runtime = runtime;
    this.state = this.buildState();
  }

  append(message: DurableStateMessage<ValuesByWireType<S>>): Promise<AppendResult> {
    return this.json.append(message);
  }

  read(
    options: ReadOptions = {},
  ): Promise<JsonReadResult<DurableStateMessage<ValuesByWireType<S>>>> {
    return this.json.read(options);
  }

  readLive(
    options: ReadLiveOptions,
  ): Promise<JsonReadLiveResult<DurableStateMessage<ValuesByWireType<S>>>> {
    return this.json.readLive(options);
  }

  metadata(): Promise<MetadataResult> {
    return this.stream.metadata();
  }

  delete(): Promise<DeleteResult> {
    return this.stream.delete();
  }

  private buildState(): DurableState<S> {
    return {
      append: (message) => this.append(message),
      insert: (type, value, options) => this.append(this.change(type, "insert", value, options)),
      update: (type, value, options) => this.append(this.change(type, "update", value, options)),
      upsert: (type, value, options) => this.append(this.change(type, "upsert", value, options)),
      delete: (type, key, options) => this.append(this.deleteMessage(type, key, options)),
      snapshotStart: (options) => this.control("snapshot-start", options),
      snapshotEnd: (options) => this.control("snapshot-end", options),
      reset: (options) => this.control("reset", options),
    };
  }

  private change<K extends keyof S & string>(
    type: K,
    operation: "insert" | "update" | "upsert",
    value: CollectionValue<S[K]>,
    options: {
      key?: string;
      oldValue?: CollectionValue<S[K]>;
      headers?: DurableStateUserHeaders;
    } = {},
  ): DurableStateMessage<ValuesByWireType<S>> {
    const def = requireByKey(this.runtime, type);
    const validated = def.codec.decode(value);
    const message: Record<string, unknown> = {
      type: def.wireType,
      key: options.key ?? keyFromValue(def, validated),
      value: validated,
      headers: { ...options.headers, operation },
    };
    if (options.oldValue !== undefined) message.old_value = def.codec.decode(options.oldValue);
    return toDurableStateMessage<S>(this.runtime, message);
  }

  private deleteMessage<K extends keyof S & string>(
    type: K,
    key: string,
    options: { oldValue?: CollectionValue<S[K]>; headers?: DurableStateUserHeaders } = {},
  ): DurableStateMessage<ValuesByWireType<S>> {
    const def = requireByKey(this.runtime, type);
    const message: Record<string, unknown> = {
      type: def.wireType,
      key,
      headers: { ...options.headers, operation: "delete" },
    };
    if (options.oldValue !== undefined) message.old_value = def.codec.decode(options.oldValue);
    return toDurableStateMessage<S>(this.runtime, message);
  }

  private control(
    control: DurableStateControl,
    options: { offset?: string; headers?: DurableStateControlHeaders } = {},
  ): Promise<AppendResult> {
    return this.append({
      headers: { ...options.headers, offset: options.offset, control },
    });
  }
}
