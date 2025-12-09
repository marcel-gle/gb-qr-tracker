/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run "npm run dev" in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run "npm run deploy" to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

// This Worker runs at Cloudflare's edge.
// It extracts the tracking ID from either the query string (?id=...) or the path (/ID, /r/ID),
// calls your Google Cloud Function with ?id=..., and passes the redirect back to the user.

// Remove the hardcoded constant - now comes from env
// const BACKEND_FUNCTION_URL = "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/redirector";

// Optional: align with your backend rules
const ID_PATTERN = /^[A-Za-z0-9_äöüÄÖÜß-]{2,100}$/;

//Helper function
async function hmacHex(keyStr: string, msg: string) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(keyStr),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sigBuf = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(msg));
  return Array.from(new Uint8Array(sigBuf))
    .map(b => b.toString(16).padStart(2, "0"))
    .join("");
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);
    const host = request.headers.get("host") || "";   // e.g., go.customer.com
    const path = url.pathname.replace(/^\/+/, "");    // e.g., "r/ID" or "ID" or ""

    // 1) Health endpoint (served instantly from the edge)
    if (path === "health") {
      return new Response("ok", {
        status: 200,
        headers: {
          "Content-Type": "text/plain",
          "Cache-Control": "no-store",
        },
      });
    }

    // 2) Resolve tracking ID from query (?id=...) OR path (/ID or /r/ID or /go/ID)
    let rawId = (url.searchParams.get("id") || "").trim();

    if (!rawId) {
      const parts = path.split("/").filter(Boolean); // ["r","TRACKING"] or ["TRACKING"]
      if (parts.length >= 2 && ["r", "go", "t"].includes(parts[0])) {
        rawId = parts[1].trim();
      } else if (parts.length >= 1 && parts[0]) {
        rawId = parts[0].trim();
      }
    }

    // Decode URL-encoded characters (ä → %C3%A4)
    let id: string | null = null;
    try {
      id = decodeURIComponent(rawId);
    } catch (e) {
      id = rawId; // fallback
    }

    // 3) Validate the ID (keep this in sync with your backend rules)
    if (!id || !ID_PATTERN.test(id)) {
      return new Response('Falsche oder fehlende persönliche ID. Zugang nur mit Einladung. Bitte geben Sie Ihre persönliche ID ein.', {
        status: 400,
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
    }

    // 4) Build the upstream request to your Google Cloud Function
    const upstream = new URL(env.BACKEND_FUNCTION_URL);  // ← Changed: now reads from env
    upstream.searchParams.set("id", id);

    // Pass context headers that may help backend logging/analytics
    const headers = new Headers(request.headers);
    headers.set("X-Original-Host", host);       // which customer domain was used
    headers.set("X-Forwarded-Proto", "https");  // clarify scheme at the edge

    // Create timestamp + HMAC signature bound to this id
    const ts = Math.floor(Date.now() / 1000).toString();
    const sig = await hmacHex(env.WORKER_HMAC_SECRET, `${ts}:${id}`);

    // attach verification headers for your Cloud Function
    headers.set("x-ts", ts);
    headers.set("x-sig", sig);
    //headers.set("x-cf-worker", "1"); // optional, just for debugging

    // 5) Fetch your function (no body needed). Keep redirects "manual" so we forward them as-is.
    const resp = await fetch(upstream.toString(), {
      method: "GET",
      headers,
      redirect: "manual",
    });

    // 6) Return your function's response as-is (typically 302/301 with Location header)
    const passthroughHeaders = new Headers(resp.headers);
    // Defensive: ensure the edge response isn't cached unless your backend says so
    if (!passthroughHeaders.has("Cache-Control")) {
      passthroughHeaders.set("Cache-Control", "no-store");
    }

    return new Response(resp.body, {
      status: resp.status,
      headers: passthroughHeaders,
    });
  },
};
