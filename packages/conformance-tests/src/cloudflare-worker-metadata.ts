/* oxlint-disable anti-slop/no-unsafe-dictionary-type, anti-slop/no-unknown-parameters, anti-slop/no-runtime-typeof, anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-unknown-returns, anti-slop/no-chained-type-assertions, anti-slop/no-known-value-widening, typescript/no-unsafe-type-assertion -- This executable host-edge collector preserves Cloudflare's evolving JSON metadata response as evidence while narrowly extracting optional documented fields; it does not expose the untyped payload to production code. */
import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { createCloudflareApi } from "alchemy/cloudflare";

type JsonRecord = Record<string, unknown>;

const asRecord = (value: unknown): JsonRecord | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as JsonRecord)
    : undefined;

const numericMember = (value: unknown, names: readonly string[]): number | null => {
  const record = asRecord(value);
  if (record === undefined) return null;
  for (const name of names) {
    if (typeof record[name] === "number") return record[name];
  }
  return null;
};

const responseJson = async (response: Response): Promise<unknown> => {
  const text = await response.text();
  if (text.length === 0) return null;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return { nonJsonBody: text.slice(0, 1_000) };
  }
};

const apiResult = (value: unknown): unknown => asRecord(value)?.result;

const latestVersionId = (value: unknown): string | undefined => {
  const result = apiResult(value);
  const items = Array.isArray(result) ? result : asRecord(result)?.items;
  if (!Array.isArray(items)) return undefined;
  const id = asRecord(items[0])?.id;
  return typeof id === "string" ? id : undefined;
};

const matchingWorker = (value: unknown, workerName: string): unknown => {
  const result = apiResult(value);
  if (!Array.isArray(result)) return undefined;
  return result.find((item) => asRecord(item)?.id === workerName);
};

async function contentEvidence(response: Response): Promise<JsonRecord> {
  const headers: Record<string, string> = {};
  response.headers.forEach((value, name) => {
    headers[name] = value;
  });
  const contentType = response.headers.get("content-type") ?? "";
  if (!response.ok || !contentType.includes("multipart/form-data")) {
    const body = await response.arrayBuffer();
    return { status: response.status, headers, downloadedBytes: body.byteLength };
  }

  const form = await response.formData();
  const parts: Array<{ name: string; bytes: number; contentType: string; file: boolean }> = [];
  form.forEach((value, name) => {
    const file = value as unknown as { readonly size?: number; readonly type?: string };
    parts.push({
      name,
      bytes:
        typeof value === "string" ? new TextEncoder().encode(value).byteLength : (file.size ?? 0),
      contentType: typeof value === "string" ? "text/plain" : (file.type ?? ""),
      file: typeof value !== "string",
    });
  });
  const modules = parts.filter(
    (part) =>
      part.file &&
      part.name !== "metadata" &&
      !part.name.endsWith(".map") &&
      part.contentType !== "application/source-map",
  );
  return {
    status: response.status,
    headers,
    partCount: parts.length,
    moduleCount: modules.length,
    downloadedModuleBytes: modules.reduce((total, part) => total + part.bytes, 0),
    parts,
  };
}

export async function captureWorkerMetadata(
  workerName: string,
  outputPath: string | undefined,
): Promise<void> {
  const queriedAt = new Date().toISOString();
  let evidence: JsonRecord;

  try {
    const api = await createCloudflareApi();
    const scriptListPath = `/accounts/${api.accountId}/workers/scripts`;
    const versionsPath = `${scriptListPath}/${encodeURIComponent(workerName)}/versions`;
    const contentPath = `${scriptListPath}/${encodeURIComponent(workerName)}/content/v2`;
    const [scriptListResponse, versionsResponse, contentResponse] = await Promise.all([
      api.get(scriptListPath),
      api.get(versionsPath),
      api.get(contentPath),
    ]);
    const scriptList = await responseJson(scriptListResponse);
    const versions = await responseJson(versionsResponse);
    const versionId = latestVersionId(versions);
    const versionDetailResponse =
      versionId === undefined ? undefined : await api.get(`${versionsPath}/${versionId}`);
    const versionDetail =
      versionDetailResponse === undefined ? undefined : await responseJson(versionDetailResponse);
    const worker = matchingWorker(scriptList, workerName);
    const detail = apiResult(versionDetail);
    const startupTimeMs =
      numericMember(detail, ["startup_time_ms"]) ?? numericMember(worker, ["startup_time_ms"]);
    const uploadedBytes = numericMember(worker, ["size", "script_size", "scriptSize"]);
    const content = await contentEvidence(contentResponse);
    const moduleCount = typeof content.moduleCount === "number" ? content.moduleCount : null;

    evidence = {
      queriedAt,
      workerName,
      endpoints: {
        scriptList: scriptListPath,
        versions: versionsPath,
        versionDetail: versionId === undefined ? null : `${versionsPath}/${versionId}`,
        content: contentPath,
      },
      responses: {
        scriptListStatus: scriptListResponse.status,
        worker,
        versionsStatus: versionsResponse.status,
        versions,
        versionDetailStatus: versionDetailResponse?.status ?? null,
        versionDetail: versionDetail ?? null,
        content,
      },
      summary: {
        uploadedBytes,
        moduleCount,
        startupTimeMs,
        uploadedBytesAvailability:
          uploadedBytes === null
            ? "unavailable: Cloudflare response omitted script size"
            : "available",
        moduleCountAvailability:
          moduleCount === null ? "unavailable: Cloudflare content was not multipart" : "available",
        startupTimeAvailability:
          startupTimeMs === null
            ? "unavailable: Cloudflare response omitted startup_time_ms"
            : "available",
      },
    };
  } catch (error) {
    evidence = {
      queriedAt,
      workerName,
      error: error instanceof Error ? error.message : String(error),
      summary: {
        uploadedBytes: null,
        moduleCount: null,
        startupTimeMs: null,
        uploadedBytesAvailability: "unavailable: metadata query failed",
        moduleCountAvailability: "unavailable: metadata query failed",
        startupTimeAvailability: "unavailable: metadata query failed",
      },
    };
  }

  if (outputPath !== undefined) {
    await mkdir(dirname(outputPath), { recursive: true });
    await writeFile(outputPath, `${JSON.stringify(evidence, null, 2)}\n`, "utf8");
    console.log(`Cloudflare metadata evidence: ${outputPath}`);
  }
  console.log(`Cloudflare metadata summary: ${JSON.stringify(evidence.summary)}`);
}
