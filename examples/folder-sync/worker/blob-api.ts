/* oxlint-disable effecttsgo/async-function -- Native Workers request/R2 streaming boundary. */
import { FILE_LIMIT } from "../server/domain.ts";
import type { Env } from "./env.ts";

const json = (error: string, status: number) => Response.json({ error }, { status });

/** The entry checks writer authorization before dispatching a PUT here. */
export async function handleBlobRequest(request: Request, env: Env): Promise<Response> {
  const hash = new URL(request.url).pathname.slice("/api/blobs/".length);

  if (!/^[a-f0-9]{64}$/.test(hash)) return json("Invalid request", 400);

  try {
    if (request.method === "HEAD") {
      const blob = await env.BLOBS.head(hash);

      return blob === null
        ? json("Blob not found", 404)
        : new Response(null, {
            headers: {
              "content-length": String(blob.size),
              "cache-control": "public, max-age=31536000, immutable",
              etag: `"${hash}"`,
            },
          });
    }

    if (request.method === "GET") {
      const blob = await env.BLOBS.get(hash);

      return blob === null
        ? json("Blob not found", 404)
        : new Response(blob.body, {
            headers: {
              "content-type": "application/octet-stream",
              "content-length": String(blob.size),
              "cache-control": "public, max-age=31536000, immutable",
              etag: `"${hash}"`,
            },
          });
    }

    if (request.method !== "PUT") return json("Method not allowed", 405);
    const declared = request.headers.get("content-length");

    // R2 single-part streaming uploads require a known byte length. No buffering or multipart fallback.
    if (declared === null) return json("Blob uploads require Content-Length", 400);

    if (!/^\d+$/.test(declared)) return json("Invalid Content-Length", 400);
    const length = Number(declared);

    if (length > FILE_LIMIT) return json("Files must be at most 64 MiB", 413);

    // No second write and no body read when the immutable content already exists.
    if ((await env.BLOBS.head(hash)) !== null) return Response.json({ hash });

    if (request.body === null) {
      if (length !== 0) return json("Missing blob body", 400);

      try {
        await env.BLOBS.put(hash, new Uint8Array(), { sha256: hash });
      } catch (error) {
        return /\(10037\)/.test(String(error))
          ? json("Blob hash mismatch", 400)
          : json("Cannot save blob", 500);
      }

      return Response.json({ hash });
    }

    // R2 needs a stream with a known length. Count every chunk before it enters
    // this fixed-length stream, preserving backpressure and never collecting bytes.
    const bounded = new FixedLengthStream(length);
    let oversized = false;
    let uploadFailed = false;
    let bodyFailed = false;
    const reader = request.body.getReader();
    const writer = bounded.writable.getWriter();

    const copy = async () => {
      let size = 0;

      try {
        while (true) {
          const chunk = await reader.read();

          if (chunk.done) break;
          size += chunk.value.byteLength;

          if (size > FILE_LIMIT) {
            oversized = true;
            throw new Error("Files must be at most 64 MiB");
          }

          await writer.write(chunk.value);
        }

        await writer.close();
      } catch (error) {
        bodyFailed = !uploadFailed;
        await writer.abort(error).catch(() => undefined);
        await reader.cancel(error).catch(() => undefined);
        throw error;
      } finally {
        reader.releaseLock();
        writer.releaseLock();
      }
    };

    const results = await Promise.allSettled([
      env.BLOBS.put(hash, bounded.readable, { sha256: hash }).catch(async (error) => {
        uploadFailed = true;
        // An early storage failure must release a copy blocked on backpressure.
        await writer.abort(error).catch(() => undefined);
        await reader.cancel(error).catch(() => undefined);
        throw error;
      }),
      copy(),
    ]);

    if (oversized) return json("Files must be at most 64 MiB", 413);
    const upload = results[0];
    const copied = results[1];

    if (bodyFailed || (copied.status === "rejected" && !uploadFailed))
      return json("Invalid blob body or Content-Length", 400);

    if (upload.status === "rejected")
      return /\(10037\)/.test(String(upload.reason))
        ? json("Blob hash mismatch", 400)
        : json("Cannot save blob", 500);

    return Response.json({ hash });
  } catch {
    return json("Blob storage unavailable", 500);
  }
}
