import { Context, Effect, Layer, Result, Schema } from "effect";
import { emptySnapshot, ResumeSnapshot } from "./fold.ts";

export type SessionStore = Pick<Storage, "getItem" | "setItem">;

export const resumeKey = (id: string) => `durable-response:v1:${id}`;

/** Offset and folded text are one record, so reload cannot restore half a batch. */
export const resumeState = (storage: SessionStore, id: string): ResumeSnapshot => {
  try {
    const saved = storage.getItem(resumeKey(id));

    if (!saved) return emptySnapshot;
    const decoded = Schema.decodeResult(Schema.fromJsonString(ResumeSnapshot))(saved);

    return Result.isSuccess(decoded) ? decoded.success : emptySnapshot;
  } catch {
    return emptySnapshot;
  }
};

export const saveResume = (storage: SessionStore, id: string, snapshot: ResumeSnapshot) => {
  try {
    storage.setItem(resumeKey(id), JSON.stringify(snapshot));
  } catch {
    /* Reading still works when storage is unavailable. */
  }
};

export const readerIdentity = (storage: SessionStore) => {
  try {
    const saved = storage.getItem("durable-response:reader");

    if (saved) return saved;
    // oxlint-disable-next-line effecttsgo/crypto-random-uuid -- Pure browser bootstrap identity; no Effect runtime exists yet.
    const id = `reader-${crypto.randomUUID().slice(0, 8)}`;
    storage.setItem("durable-response:reader", id);

    return id;
  } catch {
    // oxlint-disable-next-line effecttsgo/crypto-random-uuid -- Bootstrap fallback when sessionStorage is unavailable.
    return `reader-${crypto.randomUUID().slice(0, 8)}`;
  }
};

export class Session extends Context.Service<Session, SessionStore>()("DurableResponse/Session") {}

export const sessionLayer = Layer.sync(Session, () => ({
  getItem: (key) => window.sessionStorage.getItem(key),
  setItem: (key, value) => window.sessionStorage.setItem(key, value),
}));

export const persist = Effect.fn("Session.persist")(function* (
  id: string,
  snapshot: ResumeSnapshot,
) {
  saveResume(yield* Session, id, snapshot);
});
