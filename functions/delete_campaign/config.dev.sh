FUNCTION_NAME="delete_campaign"
ENTRY_POINT="delete_campaign"
RUNTIME="python311"
MEMORY="1GiB"
TIMEOUT="900s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( )   # add --allow-unauthenticated if it must be public

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
)

SECRETS=( )
