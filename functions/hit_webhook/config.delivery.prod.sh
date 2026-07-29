FUNCTION_NAME="hit_webhook_delivery"
ENTRY_POINT="deliver_hit"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="firestore"
TRIGGER_EVENT="google.cloud.firestore.document.v1.created"
TRIGGER_RESOURCE="projects/${PROJECT_ID}/databases/(default)/documents/hits/{document}"
TRIGGER_LOCATION="eur3"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "WEBHOOK_TIMEOUT_S=8"
)

SECRETS=( )
