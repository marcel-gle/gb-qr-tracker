FUNCTION_NAME="hit_webhook_config"
ENTRY_POINT="manage_config"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="http"
# Public invoke for browser Bearer auth; the function verifies the Firebase ID token.
TRIGGER_ARGS=( "--allow-unauthenticated" )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  "FIREBASE_PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "WEBHOOK_TIMEOUT_S=8"
)

SECRETS=( )
