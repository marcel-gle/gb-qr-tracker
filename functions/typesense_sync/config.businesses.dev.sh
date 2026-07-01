FUNCTION_NAME="typesense_sync_businesses"
ENTRY_POINT="sync_businesses"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="firestore"
TRIGGER_EVENT="google.cloud.firestore.document.v1.written"
TRIGGER_RESOURCE="projects/${PROJECT_ID}/databases/(default)/documents/businesses/{businessId}"
TRIGGER_LOCATION="eur3"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "TYPESENSE_HOST=$TYPESENSE_HOST"
  "TYPESENSE_PORT=${TYPESENSE_PORT:-443}"
  "TYPESENSE_PROTOCOL=${TYPESENSE_PROTOCOL:-https}"
  "BUSINESSES_COLLECTION=businesses_test"
)

SECRETS=(
  "TYPESENSE_API_KEY=projects/$PROJECT_ID/secrets/TYPESENSE_API_KEY:latest"
)
