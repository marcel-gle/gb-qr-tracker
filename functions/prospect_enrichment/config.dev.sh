FUNCTION_NAME="prospect_enrichment"
ENTRY_POINT="enrich_prospect"
RUNTIME="python311"
MEMORY="512Mi"
TIMEOUT="540s"

TRIGGER_KIND="firestore"
TRIGGER_EVENT="google.cloud.firestore.document.v1.created"
TRIGGER_RESOURCE="projects/${PROJECT_ID}/databases/(default)/documents/hits/{document}"
TRIGGER_LOCATION="eur3"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
)

SECRETS=(
  "SNOVIO_CLIENT_ID=projects/$PROJECT_ID/secrets/SNOVIO_CLIENT_ID:latest"
  "SNOVIO_CLIENT_SECRET=projects/$PROJECT_ID/secrets/SNOVIO_CLIENT_SECRET:latest"
  "OPENAI_API_KEY=projects/$PROJECT_ID/secrets/OPENAI_API_KEY:latest"
)

