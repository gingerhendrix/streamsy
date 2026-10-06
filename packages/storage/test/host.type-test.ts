import { Layer, Schema } from "effect";
import type { HttpClient } from "effect/http";
import { MemoryStorage, type StorageFault, Streamsy, type TransportFault } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { BunStorage } from "@streamsy/storage/bun";

class Notes extends Streamsy.json<Notes>()("StorageHost/Notes", "notes", {
  params: {},
  schema: Schema.String,
}) {}

const group = Streamsy.group(Notes);

type Assignability<To> = <From>(value: From) => From extends To ? 1 : 2;

type Equal<A, B> = Assignability<A> extends Assignability<B> ? true : false;

const services: Equal<Streamsy.Host<typeof group>, Notes | Streamsy.ChangeFeed<Notes>> = true;

/** One annotation covers a memory host and a SQLite host for the same group. */
const host = (
  filename: string,
  options: MemoryStorage.Options = {},
): Layer.Layer<Streamsy.Host<typeof group>, StorageFault> =>
  filename === ":memory:"
    ? MemoryStorage.layer(group, options)
    : BunStorage.layer(group, { client: { filename } });

const bunOptions: BunStorage.Options = { client: { filename: ":memory:" }, longPollTimeoutMs: 100 };

// @ts-expect-error BunStorage.Options requires a client.
const missingClient: BunStorage.Options = {};

// @ts-expect-error A remote host has no change feed, so it is not a Host.
const remote: Layer.Layer<
  Streamsy.Host<typeof group>,
  TransportFault,
  HttpClient.HttpClient
> = RemoteStreams.layer(group, { baseUrl: "http://localhost" });

void [services, host, bunOptions, missingClient, remote];
