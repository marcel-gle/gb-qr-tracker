FUNCTION_NAME="upload_processor"
ENTRY_POINT="process_business_upload"
RUNTIME="python311"
# Increased resources for large CSVs / heavy Firestore usage
MEMORY="2Gi"
TIMEOUT="1800s"  # 30 minutes

TRIGGER_KIND="bucket"
BUCKET_NAME="gb-qr-tracker.firebasestorage.app"   # <- adjust if different
TRIGGER_LOCATION="eu"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
)

SECRETS=(
  "MAPBOX_TOKEN=projects/$PROJECT_ID/secrets/MAPBOX_TOKEN:latest"
)
