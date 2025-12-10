FUNCTION_NAME="health_monitor"
ENTRY_POINT="health_monitor"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( )   # Requires authentication (remove --allow-unauthenticated for security)

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "TEST_LINK_ID=monitor-test-001"
  "CLOUDFLARE_WORKER_DEV_URL=https://dev.rocket-letter.de"
  "CLOUDFLARE_WORKER_PROD_URL=https://go.rocket-letter.de"
  "GCP_FUNCTION_DEV_URL=https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/redirector"
  "GCP_FUNCTION_PROD_URL=https://europe-west3-gb-qr-tracker.cloudfunctions.net/redirector"
  "ADDITIONAL_DOMAINS=ihr-brief.de,www.ihr-brief.de"
)

SECRETS=(
  "WORKER_HMAC_SECRET=projects/$PROJECT_ID/secrets/WORKER_HMAC_SECRET:latest"
)

