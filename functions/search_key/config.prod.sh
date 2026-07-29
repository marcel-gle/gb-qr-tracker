FUNCTION_NAME="search_key"
ENTRY_POINT="search_key"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="60s"

TRIGGER_KIND="http"
# Public invoke is required for Hosting rewrite + browser Bearer auth;
# the function verifies the Firebase ID token itself.
TRIGGER_ARGS=( "--allow-unauthenticated" )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  "FIREBASE_PROJECT_ID=$PROJECT_ID"
  "TYPESENSE_HOST=s17v3bieqwmltj2zp-1.a2.typesense.net"
)

SECRETS=(
  "TYPESENSE_SEARCH_ONLY_KEY=projects/$PROJECT_ID/secrets/TYPESENSE_SEARCH_ONLY_KEY:latest"
)
