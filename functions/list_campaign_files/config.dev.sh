FUNCTION_NAME="list_campaign_files"
ENTRY_POINT="list_campaign_files"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "STORAGE_BUCKET=gb-qr-tracker-dev.firebasestorage.app"
)
