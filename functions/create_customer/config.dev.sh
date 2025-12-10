FUNCTION_NAME="create_customer"
ENTRY_POINT="create_customer_http"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( "--allow-unauthenticated" )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
)

