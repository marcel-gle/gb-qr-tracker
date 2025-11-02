FUNCTION_NAME="redirector"
ENTRY_POINT="redirector"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( "--allow-unauthenticated" )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "HIT_TTL_DAYS=30"
  "GEOIP_API_URL=https://ipapi.co/{ip}/json/"
  "STORE_IP_HASH=1"
  "LOG_HIT_ERRORS=1"
)

SECRETS=(
  "IP_HASH_SALT=projects/$PROJECT_ID/secrets/IP_HASH_SALT:latest"
  "WORKER_HMAC_SECRET=projects/$PROJECT_ID/secrets/WORKER_HMAC_SECRET:latest"
)
