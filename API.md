# API Reference Documentation

Complete API reference for all GB QR Tracker endpoints and functions.

## Table of Contents

1. [Redirector API](#redirector-api)
2. [Upload Processor API](#upload-processor-api)
3. [Health Monitor API](#health-monitor-api)
4. [Create Customer API](#create-customer-api)
5. [List Campaign Files API](#list-campaign-files-api)
6. [Delete Campaign API](#delete-campaign-api)
7. [Cloudflare Worker API](#cloudflare-worker-api)

## Redirector API

### Endpoint

```
GET https://{region}-{project}.cloudfunctions.net/redirector
```

### Description

Handles link redirects and collects analytics. This is the core endpoint that processes tracking link clicks.

### Parameters

| Parameter | Type | Location | Required | Description |
|-----------|------|----------|----------|-------------|
| `id` | string | query | Yes | Tracking link ID |

### Headers

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| `x-ts` | string | Yes* | Unix timestamp (when called from Worker) |
| `x-sig` | string | Yes* | HMAC-SHA256 signature (when called from Worker) |
| `User-Agent` | string | No | Browser user agent string |
| `Referer` | string | No | HTTP referer header |
| `X-Forwarded-For` | string | No | Client IP address |

*Required when called from Cloudflare Worker (for HMAC verification)

### Request Example

```bash
# Direct call (no HMAC)
curl "https://europe-west3-gb-qr-tracker.cloudfunctions.net/redirector?id=example-link-123"

# Via Cloudflare Worker (with HMAC)
curl "https://go.rocket-letter.de/example-link-123"
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Health check endpoint (`/health`) |
| 302 | Successful redirect (Location header contains destination) |
| 400 | Invalid or missing tracking ID |
| 404 | Link not found |
| 410 | Link is inactive |
| 500 | Invalid destination URL or server error |

### Response Headers

| Header | Description |
|--------|-------------|
| `Location` | Destination URL (302 redirect) |
| `Cache-Control` | `no-store` (prevents caching) |
| `X-Content-Type-Options` | `nosniff` |
| `Referrer-Policy` | `no-referrer` |

### Response Example

```http
HTTP/1.1 302 Found
Location: https://example.com/landing
Cache-Control: no-store
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
```

### Side Effects

1. Updates `links/{linkId}`:
   - Increments `hit_count`
   - Updates `last_hit_at`

2. Creates `hits/{hitId}` document with analytics data

3. Updates `campaigns/{campaignId}`:
   - Increments `totals.hits`
   - Updates `last_hit_at`

4. Updates `customers/{uid}/businesses/{businessId}`:
   - Increments `hit_count`
   - Updates `last_hit_at`

5. Creates `campaigns/{campaignId}/unique_ips/{ip_hash}` if first visit

### Error Responses

**400 Bad Request**:
```
Missing or invalid "id" query parameter.
```

**404 Not Found**:
```
Link not found.
```

**410 Gone**:
```
Link is inactive.
```

**500 Internal Server Error**:
```
Destination is invalid or missing.
```

## Upload Processor API

### Trigger

**Event Type**: `google.cloud.storage.object.v1.finalized`

**Bucket**: `gb-qr-tracker.firebasestorage.app` (prod) or `gb-qr-tracker-dev.firebasestorage.app` (dev)

**Path Pattern**: `uploads/{env}/{uid}/{campaignId}/source/{filename}.{csv|xlsx}`

### Description

Automatically triggered when a CSV or XLSX file is uploaded to the configured GCS bucket. Processes business data and generates tracking links.

### Manifest File

Optional configuration file: `uploads/{env}/{uid}/{campaignId}/manifest.json`

**Schema**:
```json
{
  "ownerId": "firebase-uid",
  "base_url": "https://go.rocket-letter.de",
  "destination": "https://example.com/landing",
  "campaign_code": "SPRING25",
  "campaign_name": "Spring Campaign 2025",
  "campaignId": "uuid-string",
  "limit": 1000,
  "skip_existing": true,
  "geocode": true,
  "mapbox_token": "optional-override",
  "tracking_url_prefix": "https://ihr-brief.de",
  "campaign_code_from_business": false
}
```

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ownerId` | string | Yes | Firebase user UID |
| `base_url` | string | Yes | Base URL for tracking links |
| `destination` | string | No | Default destination URL |
| `campaign_code` | string | Yes | Campaign code (normalized, uppercase) |
| `campaign_name` | string | No | Campaign display name |
| `campaignId` | string | Yes | Campaign document ID |
| `limit` | number | No | Max rows to process (0 = all) |
| `skip_existing` | boolean | No | Skip existing links (default: true) |
| `geocode` | boolean | No | Enable geocoding (default: false) |
| `mapbox_token` | string | No | Mapbox token (overrides secret) |
| `tracking_url_prefix` | string | No | Prefix for printable URLs |
| `campaign_code_from_business` | boolean | No | Extract code from business data |

### CSV/XLSX Format

**Required Columns**:
- Business name (variations: `Namenszeile`, `business_name`, `company`)
- Address fields: `Straße`/`Strasse`, `Hausnummer`, `PLZ`/`Postleitzahl`, `Ort`/`Stadt`/`City`

**Optional Columns**:
- `id` or `link_id`: Custom tracking ID
- `destination` or `url`: Per-row destination URL
- `Template`: Template identifier
- `Domain`: Domain for link ID generation
- `E-Mail-Adresse`/`Email`: Email address
- `Telefonnummer`/`Phone`: Phone number
- `Entscheider 1 Vorname`, `Entscheider 1 Nachname`: Contact name

### Output Files

1. **Processed File**: `{filename}_with_links.{csv|xlsx}`
   - Original data + tracking columns:
     - `tracking_link`: Full tracking URL
     - `tracking_url`: Short printable URL
     - `tracking_id`: Tracking ID

2. **Report**: `upload_report.json`

**Report Schema**:
```json
{
  "upload_id": "campaignId-timestamp",
  "timestamp": "2025-01-15T10:30:00Z",
  "campaign": {
    "campaign_id": "uuid",
    "campaign_name": "Spring Campaign",
    "campaign_code": "SPRING25"
  },
  "input_file": {
    "name": "businesses.xlsx",
    "path": "uploads/prod/uid/campaignId/source/businesses.xlsx",
    "total_rows": 1000
  },
  "processing": {
    "started_at": "2025-01-15T10:25:00Z",
    "completed_at": "2025-01-15T10:30:00Z",
    "duration_seconds": 300.5
  },
  "statistics": {
    "total_rows": 1000,
    "processed_rows": 950,
    "successful_links": 900,
    "targets_created": 950,
    "blacklisted": {
      "count": 10,
      "businesses": [
        {
          "business_id": "blocked-business-12345",
          "business_name": "Blocked Business",
          "row_number": 5,
          "plz": "12345",
          "city": "Berlin"
        }
      ]
    },
    "skipped": {
      "count": 0,
      "reason": "limit_exceeded"
    },
    "errors": {
      "count": 5,
      "details": [
        {
          "row_number": 100,
          "business_name": "Error Business",
          "business_id": "error-business-id",
          "error": "Invalid address format",
          "error_type": "ValidationError"
        }
      ]
    },
    "excluded": {
      "count": 40,
      "reason": "no_destination"
    },
    "geocoding": {
      "enabled": true,
      "successful": 850,
      "failed": 50
    }
  },
  "output_files": {
    "with_links": "gs://bucket/path/businesses_with_links.xlsx"
  },
  "status": "completed",
  "owner_id": "firebase-uid"
}
```

### Error Handling

**Duplicate Campaign Code**:
- Returns error and deletes uploaded files
- Prevents duplicate campaign codes across customers

**Processing Errors**:
- Individual row errors logged in report
- Processing continues for other rows
- Error report generated: `upload_report_error.json`

### Example Usage

```bash
# 1. Create manifest
cat > manifest.json <<EOF
{
  "ownerId": "user-uid-123",
  "base_url": "https://go.rocket-letter.de",
  "destination": "https://example.com/landing",
  "campaign_code": "SPRING25",
  "campaign_name": "Spring Campaign",
  "campaignId": "campaign-uuid-456",
  "limit": 1000,
  "geocode": true
}
EOF

# 2. Upload files
gsutil cp businesses.xlsx gs://gb-qr-tracker.firebasestorage.app/uploads/prod/user-uid-123/campaign-uuid-456/source/
gsutil cp manifest.json gs://gb-qr-tracker.firebasestorage.app/uploads/prod/user-uid-123/campaign-uuid-456/

# 3. Wait for processing (check logs or report file)
gsutil ls gs://gb-qr-tracker.firebasestorage.app/uploads/prod/user-uid-123/campaign-uuid-456/source/
# Should see: businesses_with_links.xlsx and upload_report.json
```

## Health Monitor API

### Endpoint

```
GET https://{region}-{project}.cloudfunctions.net/health_monitor
```

### Description

Monitors system health by checking endpoints and performing test scans. Used by Cloud Scheduler for automated monitoring.

### Authentication

Requires Firebase ID token in `Authorization` header.

### Headers

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| `Authorization` | string | Yes | `Bearer {firebase_id_token}` |

### Request Example

```bash
# Using gcloud auth
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor"

# Using service account
gcloud auth activate-service-account --key-file=sa.json
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor"
```

### Response Schema

```json
{
  "timestamp": "2025-01-15T10:30:00Z",
  "overall_success": true,
  "health_checks": {
    "worker_dev": {
      "success": true,
      "error": null
    },
    "worker_prod": {
      "success": true,
      "error": null
    },
    "gcp_dev": {
      "success": true,
      "error": null
    },
    "gcp_prod": {
      "success": true,
      "error": null
    },
    "worker_ihr_brief_de": {
      "success": true,
      "error": null
    }
  },
  "test_scans": {
    "worker_dev": {
      "success": true,
      "error": null,
      "db_verified": true,
      "response": {
        "status_code": 302,
        "location": "https://example.com/monitor-test",
        "path": "worker",
        "environment": "dev"
      }
    },
    "worker_prod": {
      "success": true,
      "error": null,
      "db_verified": true,
      "response": { ... }
    },
    "direct_dev": {
      "success": true,
      "error": null,
      "db_verified": true,
      "response": {
        "status_code": 302,
        "location": "https://example.com/monitor-test",
        "path": "direct",
        "environment": "dev"
      }
    },
    "direct_prod": {
      "success": true,
      "error": null,
      "db_verified": true,
      "response": { ... }
    }
  },
  "summary": {
    "all_health_ok": true,
    "all_tests_ok": true,
    "all_db_verified": true
  }
}
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | All checks passed |
| 401 | Unauthorized (invalid token) |
| 500 | One or more checks failed |

### Health Checks

1. **Health Endpoints**: Checks `/health` on Worker and Function (dev & prod)
2. **Test Scans**: Performs actual redirects via both paths
3. **Database Verification**: Confirms hit was written to Firestore
4. **Cleanup**: Deletes test hits after verification

### Error Logging

Errors are logged with prefix `[HEALTH_MONITOR_FAIL]` for Cloud Monitoring alerts:
- Component (cloudflare_worker, gcp_function, health_monitor)
- Check type (health_check, test_scan, db_verification)
- Error message and details

## Create Customer API

### Endpoint

```
POST https://{region}-{project}.cloudfunctions.net/create_customer
```

### Description

Creates a new customer (Firebase user + Firestore document). Admin-only endpoint.

### Authentication

Requires Firebase ID token with `isAdmin: true` claim.

### Headers

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| `Authorization` | string | Yes | `Bearer {firebase_id_token}` |
| `Content-Type` | string | Yes | `application/json` |

### Request Body

```json
{
  "email": "owner@acme.com",
  "display_name": "Acme GmbH",
  "plan": "pro",
  "is_active": true,
  "set_admin": false
}
```

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `email` | string | Yes | Customer email (used for Firebase Auth) |
| `display_name` | string | No | Display name (defaults to email prefix) |
| `plan` | string | No | Plan type: `free`, `pro`, `enterprise` (default: `free`) |
| `is_active` | boolean | No | Account status (default: `true`) |
| `set_admin` | boolean | No | Set admin claim (default: `false`) |

### Response Schema

```json
{
  "ok": true,
  "uid": "firebase-uid-123",
  "user_created": true,
  "customer_created": true,
  "claims": {
    "isAdmin": false,
    "userId": "firebase-uid-123"
  }
}
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Missing required field (email) |
| 401 | Unauthorized (not admin) |
| 500 | Server error |

### Side Effects

1. Creates Firebase Auth user (if not exists)
2. Creates `customers/{uid}` document in Firestore
3. Sets custom claims (`isAdmin`, `userId`)

### Example Usage

```bash
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "owner@acme.com",
    "display_name": "Acme GmbH",
    "plan": "pro",
    "is_active": true
  }' \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/create_customer"
```

## List Campaign Files API

### Endpoint

```
POST https://{region}-{project}.cloudfunctions.net/list_campaign_files
```

### Description

Lists files (CSV, XLSX, PDF, JSON) in a campaign's storage folders and generates signed download URLs.

### Authentication

Requires Firebase ID token. User must own the campaign or be admin.

### Headers

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| `Authorization` | string | Yes | `Bearer {firebase_id_token}` |
| `Content-Type` | string | Yes | `application/json` |

### Request Body

```json
{
  "campaignId": "uuid-string",
  "env": "dev",
  "uid": "firebase-uid"
}
```

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `campaignId` | string | Yes | Campaign document ID |
| `env` | string | No | Environment: `dev` or `prod` (default: `dev`) |
| `uid` | string | No | User ID (defaults to token UID) |

### Response Schema

```json
{
  "files": [
    {
      "name": "businesses_with_links.xlsx",
      "path": "uploads/dev/uid/campaignId/source/businesses_with_links.xlsx",
      "size": 12345,
      "downloadUrl": "https://storage.googleapis.com/...?X-Goog-Algorithm=...",
      "type": "xlsx",
      "folder": "source",
      "updated": "2025-01-15T10:30:00Z"
    },
    {
      "name": "template.pdf",
      "path": "uploads/dev/uid/campaignId/templates/template.pdf",
      "size": 56789,
      "downloadUrl": "https://storage.googleapis.com/...",
      "type": "pdf",
      "folder": "templates",
      "updated": "2025-01-15T10:25:00Z"
    }
  ],
  "source": [
    {
      "name": "businesses_with_links.xlsx",
      "path": "...",
      "size": 12345,
      "downloadUrl": "...",
      "type": "xlsx",
      "folder": "source",
      "updated": "..."
    }
  ],
  "templates": [
    {
      "name": "template.pdf",
      "path": "...",
      "size": 56789,
      "downloadUrl": "...",
      "type": "pdf",
      "folder": "templates",
      "updated": "..."
    }
  ]
}
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Missing campaignId or invalid request |
| 401 | Unauthorized (invalid token) |
| 403 | Forbidden (not owner or admin) |
| 500 | Server error |

### File Types

- `csv`: CSV files
- `xlsx`: Excel files
- `pdf`: PDF templates
- `json`: JSON reports

### Signed URLs

- Valid for 1 hour
- Generated using service account credentials
- V4 signing method

### Example Usage

```bash
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "campaignId": "campaign-uuid-456",
    "env": "dev"
  }' \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/list_campaign_files"
```

## Delete Campaign API

### Endpoint

```
POST https://{region}-{project}.cloudfunctions.net/delete_campaign
```

### Description

Deletes a campaign and all associated data (cascade delete). User must own the campaign.

### Authentication

Requires Firebase ID token. User must own the campaign (or be admin).

### Headers

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| `Authorization` | string | Yes | `Bearer {firebase_id_token}` |
| `Content-Type` | string | Yes | `application/json` |

### Request Body

```json
{
  "campaignId": "uuid-string",
  "storage": {
    "bucket": "gb-qr-tracker.firebasestorage.app",
    "prefix": "uploads/prod/uid/campaignId/"
  },
  "deleteBusinesses": false,
  "dryRun": true,
  "confirm": false
}
```

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `campaignId` | string | Yes | Campaign document ID |
| `storage` | object | No | Storage cleanup config |
| `storage.bucket` | string | No | GCS bucket name |
| `storage.prefix` | string | No | Prefix to delete |
| `deleteBusinesses` | boolean | No | Delete unused businesses (default: `false`) |
| `dryRun` | boolean | No | Preview only (default: `false`) |
| `confirm` | boolean | Yes | Confirmation flag (required for actual deletion) |

### Response Schema (Dry Run)

```json
{
  "ok": true,
  "dryRun": true,
  "plan": {
    "counts": {
      "targets": 1000,
      "uniqueIps": 500,
      "links": 950,
      "hits": 5000,
      "businessesToMaybeDelete": 0,
      "businessesPrunable": 0,
      "campaignDoc": 1,
      "storage": 5
    },
    "storage": {
      "bucket": "gb-qr-tracker.firebasestorage.app",
      "prefix": "uploads/prod/uid/campaignId/"
    }
  }
}
```

### Response Schema (Actual Deletion)

```json
{
  "ok": true,
  "deleted": {
    "hits": 5000,
    "targets": 1000,
    "unique_ips": 500,
    "links": 950,
    "businesses": 0,
    "campaignDoc": 1,
    "bucket_name": "gb-qr-tracker.firebasestorage.app",
    "storage_prefix": "uploads/prod/uid/campaignId/",
    "storageBlobs": 5
  }
}
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Success (dry run or deletion) |
| 400 | Missing campaignId |
| 401 | Unauthorized |
| 403 | Forbidden (not owner) |
| 500 | Server error |

### Deletion Order

1. Hits (from `hits` collection)
2. Targets (from `campaigns/{id}/targets`)
3. Unique IPs (from `campaigns/{id}/unique_ips`)
4. Links (from `links` collection)
5. Businesses (optional, only if unused elsewhere)
6. Campaign document
7. Storage files (if configured)

### Example Usage

```bash
# Dry run first
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "campaignId": "campaign-uuid-456",
    "storage": {
      "bucket": "gb-qr-tracker.firebasestorage.app",
      "prefix": "uploads/prod/uid/campaignId/"
    },
    "dryRun": true,
    "confirm": false
  }' \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/delete_campaign"

# Actual deletion
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "campaignId": "campaign-uuid-456",
    "storage": {
      "bucket": "gb-qr-tracker.firebasestorage.app",
      "prefix": "uploads/prod/uid/campaignId/"
    },
    "confirm": true
  }' \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/delete_campaign"
```

## Cloudflare Worker API

### Endpoints

**Health**: `GET https://{domain}/health`

**Redirect**: `GET https://{domain}/{tracking-id}` or `GET https://{domain}?id={tracking-id}`

### Health Endpoint

**Response**: `200 OK` with body `ok`

**Example**:
```bash
curl https://go.rocket-letter.de/health
# → ok
```

### Redirect Endpoint

**ID Extraction**:
- Query parameter: `?id=TRACKING-ID`
- Path: `/TRACKING-ID` or `/r/TRACKING-ID` or `/go/TRACKING-ID`

**Headers Added**:
- `x-ts`: Unix timestamp
- `x-sig`: HMAC-SHA256 signature

**Response**: Passes through GCP Function response (typically 302 redirect)

**Example**:
```bash
curl -v https://go.rocket-letter.de/example-link-123
# → 302 Redirect to destination
```

### Error Responses

**400 Bad Request**:
```
Falsche oder fehlende persönliche ID. Zugang nur mit Einladung. Bitte geben Sie Ihre persönliche ID ein.
```

(German: "Invalid or missing personal ID. Access only by invitation. Please enter your personal ID.")

