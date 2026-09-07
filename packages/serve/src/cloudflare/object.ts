import type { AlarmInvocationInfo } from "@cloudflare/workers-types";
import { DurableObject } from "cloudflare:workers";
import { Context, Layer } from "effect";
import type { Storage, StorageFault, StreamsReader, StreamsWriter } from "@streamsy/core";
import { makeEdge, type HttpOptions } from "@streamsy/core/http";
import { Alarm, alarmLayer } from "./alarm.ts";
import { HostCommand } from "./host-command.ts";
import { hostProgram } from "./host-program.ts";

const unavailable = (): Response =>
  new Response("Storage unavailable", {
    status: 503,
    headers: {
      "retry-after": "1",
      "x-content-type-options": "nosniff",
      "cross-origin-resource-policy": "cross-origin",
    },
  });

type Edge = ReturnType<typeof makeEdge<StorageFault, Storage | Alarm>>;

export abstract class StreamsyObject<Env = unknown> extends DurableObject<Env> {
  #edge: Edge | undefined;

  abstract layer(): Layer.Layer<StreamsReader | StreamsWriter | Storage, StorageFault>;

  options(): HttpOptions {
    return {};
  }

  #getEdge(): Edge {
    return (this.#edge ??= (() => {
      const options = this.options();
      return makeEdge<StorageFault, Storage | Alarm>(
        options,
        this.layer().pipe(Layer.provideMerge(alarmLayer(this.ctx.storage))),
        hostProgram(options),
      );
    })());
  }

  #recover(edge: Edge): Promise<Response> {
    if (this.#edge !== edge) return Promise.resolve(unavailable());
    this.#edge = undefined;
    return edge
      .dispose()
      .catch(() => undefined)
      .then(() => unavailable());
  }

  override fetch(request: Request): Promise<Response> {
    let edge: Edge;
    try {
      edge = this.#getEdge();
    } catch {
      return Promise.resolve(unavailable());
    }
    return edge.handler(request).catch(() => this.#recover(edge));
  }

  override alarm(info?: AlarmInvocationInfo): Promise<void> {
    const edge = this.#getEdge();
    return edge
      .handler(
        new Request("https://streamsy.internal/alarm", { method: "POST" }),
        Context.make(HostCommand, { _tag: "ExpireDue" }),
      )
      .then((response) => {
        if (response.status < 200 || response.status >= 300)
          throw new Error(`Streamsy expiry alarm failed: ${response.status}`);
        void info;
      })
      .catch((error) =>
        this.#recover(edge).then(() => {
          throw error;
        }),
      );
  }
}
