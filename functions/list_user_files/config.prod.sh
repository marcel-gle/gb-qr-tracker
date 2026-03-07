FUNCTION_NAME="list_user_files"
ENTRY_POINT="list_user_files"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "STORAGE_BUCKET=gb-qr-tracker.firebasestorage.app"
)

SECRETS=( )
