FUNCTION_NAME="migrate_document"
ENTRY_POINT="migrate_document"
RUNTIME="python311"
MEMORY="512Mi"
TIMEOUT="540s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( --allow-unauthenticated )

# Environment variables for prod
# NOTE: For production, strongly prefer injecting GOOGLE_*/CANVA_* via Secret Manager or CI,
# not hardcoding them here.
ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  "REGION=$REGION"
  "GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID"
  "GOOGLE_CLIENT_SECRET=$GOOGLE_CLIENT_SECRET"
  "GOOGLE_REDIRECT_URI=$GOOGLE_REDIRECT_URI"
  "CANVA_CLIENT_ID=$CANVA_CLIENT_ID"
  "CANVA_CLIENT_SECRET=$CANVA_CLIENT_SECRET"
  "CANVA_REDIRECT_URI=$CANVA_REDIRECT_URI"
)

SECRETS=(
)


