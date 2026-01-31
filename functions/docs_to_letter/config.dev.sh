FUNCTION_NAME="migrate_document"
ENTRY_POINT="migrate_document"
RUNTIME="python311"
MEMORY="512Mi"
TIMEOUT="540s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( --allow-unauthenticated )

# Environment variables for dev
ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  "REGION=$REGION"
  "TEMPLATED_IO_KEY=$TEMPLATED_IO_KEY"
)

# For MVP we do not use Secret Manager; recommend moving secrets there in prod.
SECRETS=(

)



