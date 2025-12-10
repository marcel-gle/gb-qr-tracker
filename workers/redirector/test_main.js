var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.ts
var ID_PATTERN = /^[A-Za-z0-9_äöüÄÖÜß-]{2,100}$/;

async function hmacHex(keyStr, msg) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(keyStr),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sigBuf = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(msg));
  return Array.from(new Uint8Array(sigBuf)).map((b) => b.toString(16).padStart(2, "0")).join("");
}
__name(hmacHex, "hmacHex");
var index_default = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const host = request.headers.get("host") || "";
    const path = url.pathname.replace(/^\/+/, "");
    if (path === "health") {
      return new Response("ok", {
        status: 200,
        headers: {
          "Content-Type": "text/plain",
          "Cache-Control": "no-store"
        }
      });
    }

    let rawId = (url.searchParams.get("id") || "").trim();

    if (!rawId) {
      const parts = path.split("/").filter(Boolean);
      if (parts.length >= 2 && ["r", "go", "t"].includes(parts[0])) {
        rawId = parts[1].trim();
      } else if (parts.length >= 1 && parts[0]) {
        rawId = parts[0].trim();
      }
    }
    
    // Decode URL-encoded characters (ä → %C3%A4)
    let id = null;
    try {
      id = decodeURIComponent(rawId);
    } catch (e) {
      id = rawId; // fallback
    }
    
    if (!id || !ID_PATTERN.test(id)) {
      return new Response(
        "Falsche oder fehlende persönliche ID. Zugang nur mit Einladung. Bitte geben Sie Ihre persönliche ID ein.",
        {
          status: 400,
          headers: { "Content-Type": "text/plain; charset=utf-8" }
        }
      );
    }


    const upstream = new URL(env.BACKEND_FUNCTION_URL);
    upstream.searchParams.set("id", id);
    const headers = new Headers(request.headers);
    headers.set("X-Original-Host", host);
    headers.set("X-Forwarded-Proto", "https");
    const ts = Math.floor(Date.now() / 1e3).toString();
    const sig = await hmacHex(env.WORKER_HMAC_SECRET, `${ts}:${id}`);
    headers.set("x-ts", ts);
    headers.set("x-sig", sig);
    const resp = await fetch(upstream.toString(), {
      method: "GET",
      headers,
      redirect: "manual"
    });
    const passthroughHeaders = new Headers(resp.headers);
    if (!passthroughHeaders.has("Cache-Control")) {
      passthroughHeaders.set("Cache-Control", "no-store");
    }
    return new Response(resp.body, {
      status: resp.status,
      headers: passthroughHeaders
    });
  }
};
export {
  index_default as default
};
//# sourceMappingURL=index.js.map
