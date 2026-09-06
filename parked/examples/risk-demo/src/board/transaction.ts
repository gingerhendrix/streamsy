/**
 * Identity of the board-projection transaction that applies a command.
 *
 * The guarantee this rests on is one-directional, and worth stating precisely:
 * **a transaction never contains part of a command.** A command's `decide`
 * emits its events in one atomic canonical append, a catch-up read returns whole
 * messages up to the durable head, and the head only ever advances by a complete
 * append — so a delivery boundary can only end where an append ended.
 *
 * The converse does *not* hold. A boundary is whatever the reader found
 * unread, so a projector catching up on a backlog will fold several commands
 * into one transaction. That is harmless for waiting: a transaction carrying
 * this id has applied every effect of that command (along with, possibly, other
 * commands' effects), which is exactly what a client waiting on its own command
 * needs to know. What it is not is a promise of one transaction per command.
 *
 * This is why the id no longer carries a canonical offset. Under the previous
 * per-event projection a command could span several transitions, so the offset
 * was there to pick out the final one. The transaction boundary now gives that
 * guarantee structurally, and an offset in the id would describe only *which
 * batch the projector happened to read* — something the client holding the
 * acknowledgement cannot know, and no longer needs to.
 */
export function boardProjectionTxId(commandId: string): string {
  return `risk-board:${encodeURIComponent(commandId)}`;
}

/**
 * The transaction a client waits for after its own command. The ack deliberately
 * carries no `txid` — it is derivable, so publishing it would be a second
 * spelling of the same fact rather than information the client lacked.
 */
export function ackTxId(ack: { commandId: string }): string {
  return boardProjectionTxId(ack.commandId);
}
