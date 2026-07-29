# `search_key` Cloud Function

Mints Typesense **scoped search keys** for the logged-in Firebase user so the browser
never holds the parent search-only key.

## Behaviour

1. Verifies `Authorization: Bearer <Firebase ID token>` via Firebase Admin.
2. Reads `uid` from the token.
3. Generates two scoped keys (local HMAC; no Typesense network call):
   - `businesses` → `filter_by: ownerIds:=<uid>`
   - `customer_businesses` → `filter_by: owner_id:=<uid>`
4. Returns `{ host, businessesKey, customerBusinessesKey }`.

Deployed as Gen2 HTTP in `europe-west3`, entry point `search_key`.

## One-time setup (per GCP project)

Projects: `gb-qr-tracker-dev` (dev) and `gb-qr-tracker` (prod).

### 1. Typesense search-only key

Create an API key with **only** `documents:search` permissions.
Do **not** use the admin `TYPESENSE_API_KEY` used by `typesense_sync` — admin keys
cannot mint scoped keys (searches return `401`).

### 2. Secret Manager

```bash
# Create (or add a new version) — paste the search-only key when prompted:
printf '%s' 'YOUR_SEARCH_ONLY_KEY' | gcloud secrets create TYPESENSE_SEARCH_ONLY_KEY \
  --project=gb-qr-tracker-dev \
  --data-file=-
# (use `secrets versions add` if the secret already exists)

# Grant the Cloud Functions service account access:
gcloud secrets add-iam-policy-binding TYPESENSE_SEARCH_ONLY_KEY \
  --project=gb-qr-tracker-dev \
  --member="serviceAccount:cf-campaign-importer@gb-qr-tracker-dev.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

Repeat for prod (`gb-qr-tracker` / matching SA email).

### 3. `TYPESENSE_HOST`

Host only (no `https://`, no port), e.g. `xxx.a1.typesense.net`.

- **Local deploy:** set in `.env.dev` / `.env.prod`
- **CI:** set GitHub Actions secret `TYPESENSE_HOST` (used by deploy-dev / deploy-prod workflows)

## Deploy

```bash
./deploy.sh dev search_key
./deploy.sh prod search_key
```

Or push to `dev` / `main` — CI deploys when `functions/search_key/**` changes.

## Frontend rewrite (separate repo)

In `gb-qr-tracking-svelte/firebase.json`, before the `"source": "**"` catch-all:

```json
{
  "source": "/api/search-key",
  "function": "search_key",
  "region": "europe-west3"
}
```

## Verify

```bash
# Expect 401 — function is live and enforcing auth
curl -i -X POST \
  https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/search_key
# → {"error":"Missing bearer token"}
```

In the app: log in, search (≥ 3 characters). Network tab should show
`POST /api/search-key` → 200 with `host` + two keys, then successful Typesense searches.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Typesense searches return `401` | Parent key is an **admin** key — use search-only |
| `500` "Typesense env vars are not configured" | Missing `TYPESENSE_HOST` or `TYPESENSE_SEARCH_ONLY_KEY` |
| `500` "FIREBASE_PROJECT_ID is not configured" | Missing project env on the function |
| `401` for logged-in users | Dev/prod project mismatch (`FIREBASE_PROJECT_ID`) |
| `404` via Hosting | Rewrite missing/misnamed, or function not in `europe-west3` |
| Keys 200 but empty results | Docs missing `ownerIds` / `owner_id`, or wrong collection names |
