/** Release language inventory. Kept outside the site so the scanner scans its own scope honestly. */
export const staleTerms = [
  ...["http-client", "state", "streams", "tanstack-db", "sinks", "core/json", "derive"].map(
    (name) => `@streamsy/${name}`,
  ),
  "StateProjection",
  "JsonCodec",
  "createStreamProtocol",
  "createMemoryStorageAdapter",
  "createHttpHandler",
  "directProtocolClient",
  "StorageAdapter",
  "bindStream",
  "streamIdentity",
  "mesh",
  "0.3.0",
  "0.2.1",
  "AfterCommit",
  "AdapterFault",
  "awaitChange",
  "runAwaitChangeLoop",
  "NotSupportedError",
  "scheduleExpiry",
  "cancelExpiry",
  "layerRouted",
  "Backend.make",
];

/** Retired names from the approved batch 1, 2 and 3 renames. */
export const retiredCoreNames = [
  "Projection.make",
  "Projection.serialized",
  "Projection.onChange",
  "Serve.document",
  "OnChangeOptions",
  "StreamFamily",
  "StreamGroup",
  "AnyFamily",
  "AnyStream",
  "FamilyAccess",
  "FamilyRoute",
  "provides(",
  "Streamsy.Ref",
  "StreamRef",
  "Changes<",
  "ChangesApi",
  "State.delete",
  "@streamsy/core/Family/",
  "Projection.family",
  ".member(",
];

/** Retired names from the batch 4 renames: projection kinds and value serving. */
export const retiredProjectionNames = [
  "Projection.fused",
  "FusedDefinition",
  '"Fused"',
  "Projection.stream(",
  "StreamDefinition",
  "DocumentOptions",
  "DOCUMENT_CONTRACT_HEADER",
  "x-streamsy-document-contract",
  // Phase 2: one place for state and one read.
  "Projection.loadState",
  "Projection.fold",
  "OutputFold",
  "Output.value",
  "progress.record",
  "Pick<CheckpointRecord",
];
retiredCoreNames.push(...retiredProjectionNames);
staleTerms.push(...retiredCoreNames);
