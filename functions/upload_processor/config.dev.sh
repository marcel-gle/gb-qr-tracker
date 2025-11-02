FUNCTION_NAME="upload_processor"
ENTRY_POINT="process_business_upload"
RUNTIME="python311"
MEMORY="1GiB"
TIMEOUT="540s"

TRIGGER_KIND="bucket"
BUCKET_NAME="gb-qr-tracker-dev.firebasestorage.app"
TRIGGER_LOCATION="eu"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
)

SECRETS=(
  "MAPBOX_TOKEN=projects/$PROJECT_ID/secrets/MAPBOX_TOKEN:latest"
)
