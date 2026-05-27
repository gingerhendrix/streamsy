/**
 * Helper used by adapter authors to assemble a protocol-facing `Stream`
 * from bound record/message/producer/runtime dependencies. The resulting
 * `Stream` implements the record/message operations directly and forwards
 * each call to the corresponding bound dependency. Optional behaviour is
 * surfaced as additional members.
 */
import type { ComposedStreamDeps, Stream } from "../types/factory.ts";

export function composeStream(deps: ComposedStreamDeps): Stream {
  const stream: Stream = {
    id: deps.id,
    getRecord: () => deps.recordStore.getRecord(),
    createRecord: (record) => deps.recordStore.createRecord(record),
    updateRecord: (patch) => deps.recordStore.updateRecord(patch),
    deleteRecord: () => deps.recordStore.deleteRecord(),
    appendMessages: (messages) => deps.messageStore.appendMessages(messages),
    listMessages: (options) => deps.messageStore.listMessages(options),
    deleteMessages: () => deps.messageStore.deleteMessages(),
    producers: deps.producerStore,
    references: deps.referenceTracker,
    mutations: deps.mutations,
    events: deps.events,
    expiry: deps.expiry,
  };
  return stream;
}
