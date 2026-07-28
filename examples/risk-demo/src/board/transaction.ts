/**
 * Identity for one canonical event's board-projection transition.
 *
 * Commands can emit several events with the same commandId, so including the
 * canonical offset lets a command ack identify its final projected transition
 * without pretending that the causal source offset is itself a transaction id.
 */
export function boardProjectionTxId(commandId: string, sourceOffset: string): string {
  return `risk-board:${encodeURIComponent(commandId)}:${encodeURIComponent(sourceOffset)}`;
}

/**
 * The transition a client waits for after its own command. The ack deliberately
 * carries no `txid` (C8) — it is derivable, so publishing it would be a second
 * spelling of the same fact rather than information the client lacked.
 */
export function ackTxId(ack: { commandId: string; eventOffset: string }): string {
  return boardProjectionTxId(ack.commandId, ack.eventOffset);
}
