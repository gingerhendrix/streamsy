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

/** Approved batch 1 names. Projection renames belong to later batches. */
export const retiredCoreNames = [
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
];
staleTerms.push(...retiredCoreNames);
