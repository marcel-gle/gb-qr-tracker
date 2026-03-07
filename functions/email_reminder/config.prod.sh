FUNCTION_NAME="email_reminder"
ENTRY_POINT="run_reminders"
RUNTIME="python311"
MEMORY="256Mi"
TIMEOUT="300s"

TRIGGER_KIND="http"
TRIGGER_ARGS=( )

ENV_VARS=(
  "PROJECT_ID=$PROJECT_ID"
  'DATABASE_ID=(default)'
  "SMTP_HOST=smtp-relay.gmail.com"
  "SMTP_PORT=587"
  "SMTP_USER=marcel.gleich@gleich-brother.de"
  "FROM_EMAIL=marcel.gleich@gleich-brother.de"
  "FROM_NAME=Rocket Letter"
)

SECRETS=(
  "REMINDER_SECRET=projects/$PROJECT_ID/secrets/REMINDER_SECRET:latest"
  "SMTP_APP_PASSWORD=projects/$PROJECT_ID/secrets/SMTP_APP_PASSWORD:latest"
)
