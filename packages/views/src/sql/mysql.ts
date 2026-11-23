import { Array } from "effect";
import type { ServerDialect } from "./server.ts";

const text = "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

export const mysql = {
  ddl: [
    `CREATE TABLE IF NOT EXISTS streamsy_view_layout (
      id SMALLINT PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_circuits (
      ns BINARY(32) PRIMARY KEY, namespace ${text} NOT NULL, plan_hash ${text} NOT NULL) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_integrals (
      ns BINARY(32) NOT NULL, relation VARBINARY(255) NOT NULL, row_hash BINARY(32) NOT NULL,
      row_key LONGBLOB NOT NULL, value ${text} NOT NULL,
      PRIMARY KEY (ns, relation, row_hash)) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_arrangements (
      ns BINARY(32) NOT NULL, arrangement VARBINARY(255) NOT NULL, index_hash BINARY(32) NOT NULL,
      ord_prefix VARBINARY(1024) NOT NULL, row_hash BINARY(32) NOT NULL,
      sort_key ${text} NOT NULL, row_key LONGBLOB NOT NULL,
      PRIMARY KEY (ns, arrangement, index_hash, ord_prefix, row_hash)) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_accumulators (
      ns BINARY(32) NOT NULL, node VARBINARY(255) NOT NULL, group_hash BINARY(32) NOT NULL,
      group_key ${text} NOT NULL, value ${text} NOT NULL,
      PRIMARY KEY (ns, node, group_hash)) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_key_owners (
      ns BINARY(32) NOT NULL, relation VARBINARY(255) NOT NULL, output_hash BINARY(32) NOT NULL,
      output_key LONGBLOB NOT NULL, owner_key LONGBLOB NOT NULL,
      PRIMARY KEY (ns, relation, output_hash)) ENGINE=InnoDB`,
  ],
  upsert: (_keys, updates) =>
    `ON DUPLICATE KEY UPDATE ${Array.join(
      Array.map(updates, (column) => `${column} = VALUES(${column})`),
      ", ",
    )}`,
  doNothing: "ON DUPLICATE KEY UPDATE row_hash = row_hash",
} satisfies Pick<ServerDialect, "ddl" | "upsert" | "doNothing">;
