/**
 * The board projection puts this receipt on the last State row for a command.
 * A browser can observe the receipt only after every earlier row was applied.
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
