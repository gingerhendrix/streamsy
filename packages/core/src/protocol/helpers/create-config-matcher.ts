/**
 * Create-config compatibility check for idempotent stream creation.
 *
 * Responsibilities:
 *
 * - Content type defaults to `application/octet-stream` when the caller omits
 *   it AND the existing record has none recorded.
 * - `forkedFrom` must match exactly (including absent ↔ absent).
 * - `forkOffset` must match when the caller specifies one; an unspecified
 *   `forkOffset` is treated as "don't care".
 * - `forkSubOffset` must match, treating an absent header and `0` as equal so a
 *   sub-offset of `0` is idempotent with a plain fork.
 * - Fork callers may inherit the source's expiry: when `forkedFrom` is set
 *   and both `ttlSeconds` and `expiresAt` are omitted, the existing record's
 *   expiry fields are accepted.
 * - Closed-state must agree (existing.closed === options.closed).
 *
 * Used by `CreateStreamService` when an incoming create request targets an
 * existing stream and must be classified as idempotent or conflicting.
 */

import type { StreamRecord } from "../../types/storage.ts";
import type { CreateOptions } from "../../types/protocol.ts";
import { contentTypeMatches } from "./content-type-matcher.ts";

export function configMatches(existing: StreamRecord, options: CreateOptions): boolean {
  const contentType =
    options.contentType ?? existing.config.contentType ?? "application/octet-stream";
  if (!contentTypeMatches(existing.config.contentType, contentType)) return false;
  if ((options.forkedFrom ?? undefined) !== existing.lifecycle.forkedFrom) return false;
  if (options.forkOffset !== undefined && options.forkOffset !== existing.lifecycle.forkOffset)
    return false;
  if ((options.forkSubOffset ?? 0) !== (existing.lifecycle.forkSubOffset ?? 0)) return false;
  const inheritedForkExpiry =
    options.forkedFrom && options.ttlSeconds === undefined && options.expiresAt === undefined;
  if (existing.config.ttlSeconds !== options.ttlSeconds && !inheritedForkExpiry) return false;
  if (existing.config.expiresAt !== options.expiresAt && !inheritedForkExpiry) return false;
  return (existing.lifecycle.closed === true) === (options.closed === true);
}
