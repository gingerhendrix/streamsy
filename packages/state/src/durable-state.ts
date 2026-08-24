import {
  JsonProtocol,
  JsonStream,
  normalizeJsonCodec,
  type JsonCodec,
  type JsonReadNextResult,
  type JsonReadResult,
  type JsonSchema,
} from "@streamsy/json";
import type {
  AppendResult,
  CreateOptions,
  DeleteResult,
  MetadataResult,
  ProtocolStream,
  ReadNextOptions,
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

export type DurableStateValue = {} | null | undefined;
export type DurableStateMessage<RowMap extends object> =
  | { [Type in keyof RowMap & string]: ChangeMessage<Type, RowMap[Type]> }[keyof RowMap & string]
  | ControlMessage;

export interface DurableStateCollectionDef<T> {
  schema: JsonSchema<T>;
  type?: string;
  primaryKey: string | ((value: T) => string);
}

export type DurableStateSchemaMap<S extends object = object> = {
  [K in keyof S]: DurableStateCollectionDef<CollectionValue<S[K]>>;
};
export type CollectionValue<Def> = Def extends DurableStateCollectionDef<infer T> ? T : never;
export type ValuesByWireType<S extends DurableStateSchemaMap<S>> = {
  [K in keyof S as S[K]["type"] extends string ? S[K]["type"] : K & string]: CollectionValue<S[K]>;
};

export type DurableStateCreateOptions = Omit<CreateOptions, "contentType" | "initialData">;
export type DurableStateCreateResult<S extends DurableStateSchemaMap<S>> =
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
export type DurableStateGetResult<S extends DurableStateSchemaMap<S>> =
  | { status: "ok"; stream: DurableStateStream<S> }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "not-supported"; feature: string; message?: string }
  | { status: "content-type-conflict"; contentType: string; expectedContentType: string };

interface PreparedCollectionValue {
  readonly key: string;
  readonly value: DurableStateValue;
}

interface CollectionRuntime {
  readonly key: string;
  readonly wireType: string;
  decode(value: DurableStateValue): DurableStateValue;
  prepare(value: DurableStateValue, explicitKey?: string): PreparedCollectionValue;
}

interface DurableStateWireHeaders {
  readonly control?: DurableStateValue;
  readonly operation?: DurableStateValue;
}

interface DurableStateWireMessage {
  readonly headers?: DurableStateValue;
  readonly type?: DurableStateValue;
  readonly key?: DurableStateValue;
  readonly value?: DurableStateValue;
  readonly old_value?: DurableStateValue;
}

interface MutableDurableStateChangeMessage {
  type: string;
  key: string;
  value?: DurableStateValue;
  old_value?: DurableStateValue;
  headers: DurableStateChangeHeaders;
}

function isWireObject(value: DurableStateValue): boolean {
  return (
    value !== null &&
    Object(value) === value &&
    Object.prototype.toString.call(value) !== "[object Function]" &&
    !Array.isArray(value)
  );
}

function isWireMessage(value: DurableStateValue): value is DurableStateWireMessage {
  return isWireObject(value);
}

function isWireHeaders(value: DurableStateValue): value is DurableStateWireHeaders {
  return isWireObject(value);
}

function isWireString(value: DurableStateValue): value is string {
  return (
    value !== null && value !== undefined && Object(value) !== value && value.constructor === String
  );
}

function isPrimaryKeyFunction<T>(
  primaryKey: string | ((value: T) => string),
): primaryKey is (value: T) => string {
  return primaryKey.constructor === Function;
}

function parseOperation(value: DurableStateValue): DurableStateOperationWithExtensions {
  if (!isWireString(value)) throw new Error("Invalid Durable State operation");
  switch (value) {
    case "insert":
    case "update":
    case "upsert":
    case "delete":
      return value;
    default:
      throw new Error("Invalid Durable State operation");
  }
}

function parseControl(value: DurableStateValue): DurableStateControl {
  if (!isWireString(value)) throw new Error("Invalid Durable State control message");
  switch (value) {
    case "snapshot-start":
    case "snapshot-end":
    case "reset":
      return value;
    default:
      throw new Error("Invalid Durable State control message");
  }
}

function buildRuntime<S extends DurableStateSchemaMap<S>>(schema: S): CollectionRuntime[] {
  const runtime: CollectionRuntime[] = [];
  for (const key in schema) {
    const def = schema[key];
    const codec = normalizeJsonCodec(def.schema);
    const primaryKey = def.primaryKey;
    runtime.push({
      key,
      wireType: def.type ?? key,
      decode(value) {
        return codec.decode(value);
      },
      prepare(value, explicitKey) {
        const decoded = codec.decode(value);
        if (explicitKey !== undefined) return { key: explicitKey, value: decoded };
        if (isPrimaryKeyFunction(primaryKey)) {
          return { key: primaryKey(decoded), value: decoded };
        }
        return { key: keyFromValue(primaryKey, decoded), value: decoded };
      },
    });
  }
  return runtime;
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

function keyFromValue(primaryKey: string, value: DurableStateValue): string {
  if (!isWireMessage(value))
    throw new Error(`Cannot extract primary key ${primaryKey} from non-object value`);
  let owner: object | null = value;
  let key: DurableStateValue;
  while (owner !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(owner, primaryKey);
    if (descriptor !== undefined) {
      key = "value" in descriptor ? descriptor.value : descriptor.get?.call(value);
      break;
    }
    owner = Object.getPrototypeOf(owner);
  }
  if (!isWireString(key)) throw new Error(`Primary key ${primaryKey} must be a string`);
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
function validateMessage(
  runtime: CollectionRuntime[],
  value: DurableStateValue,
): DurableStateWireMessage {
  if (!isWireMessage(value)) throw new Error("Durable State message must be an object");
  const headers = value.headers;
  if (!isWireHeaders(headers)) throw new Error("Durable State message requires headers object");

  if ("control" in headers) {
    parseControl(headers.control);
    if ("type" in value || "key" in value) {
      throw new Error("Durable State control messages must not include type or key");
    }
    return value;
  }

  const type = value.type;
  const key = value.key;
  if (!isWireString(type) || type.length === 0)
    throw new Error("Change message type must be a string");
  if (!isWireString(key) || key.length === 0)
    throw new Error("Change message key must be a string");
  const operation = parseOperation(headers.operation);

  const def = requireByWireType(runtime, type);

  if (operation === "insert" || operation === "update" || operation === "upsert") {
    if (!Object.hasOwn(value, "value")) throw new Error(`${operation} message requires value`);
    def.decode(value.value);
  } else if (Object.hasOwn(value, "value") && value.value !== null) {
    throw new Error("delete message value must be null when present");
  }
  if (Object.hasOwn(value, "old_value")) def.decode(value.old_value);
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
function toDurableStateMessage<S extends DurableStateSchemaMap<S>>(
  runtime: CollectionRuntime[],
  value: DurableStateValue,
): DurableStateMessage<ValuesByWireType<S>> {
  const validated = validateMessage(runtime, value);
  // SAFETY: `validateMessage` proves the schema-to-runtime invariant documented above.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Runtime-proven by `validateMessage`; see the invariant documented above.
  return validated as DurableStateMessage<ValuesByWireType<S>>;
}

function durableStateCodec<S extends DurableStateSchemaMap<S>>(
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

export interface DurableState<S extends DurableStateSchemaMap<S>> {
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

export class DurableStateProtocol<S extends DurableStateSchemaMap<S>> {
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

export function createDurableStateProtocol<S extends DurableStateSchemaMap<S>>(
  protocol: StreamProtocolFactory,
  schema: S,
): DurableStateProtocol<S> {
  return new DurableStateProtocol(protocol, schema);
}

export class DurableStateStream<S extends DurableStateSchemaMap<S>> {
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

  readNext(
    options: ReadNextOptions,
  ): Promise<JsonReadNextResult<DurableStateMessage<ValuesByWireType<S>>>> {
    return this.json.readNext(options);
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
    const prepared = def.prepare(value, options.key);
    const message: MutableDurableStateChangeMessage = {
      type: def.wireType,
      key: prepared.key,
      value: prepared.value,
      headers: { ...options.headers, operation },
    };
    if (options.oldValue !== undefined) message.old_value = def.decode(options.oldValue);
    return toDurableStateMessage<S>(this.runtime, message);
  }

  private deleteMessage<K extends keyof S & string>(
    type: K,
    key: string,
    options: { oldValue?: CollectionValue<S[K]>; headers?: DurableStateUserHeaders } = {},
  ): DurableStateMessage<ValuesByWireType<S>> {
    const def = requireByKey(this.runtime, type);
    const message: MutableDurableStateChangeMessage = {
      type: def.wireType,
      key,
      headers: { ...options.headers, operation: "delete" },
    };
    if (options.oldValue !== undefined) message.old_value = def.decode(options.oldValue);
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
