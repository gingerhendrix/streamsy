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
