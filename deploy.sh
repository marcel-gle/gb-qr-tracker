#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:?Usage: ./deploy.sh <dev|prod> <function_dir>}"
FUNC_DIR="${2:?Usage: ./deploy.sh <dev|prod> <function_dir>}"

# 1) Load env (PROJECT_ID, REGION, SA, etc.)
if [[ -f ".env.${ENVIRONMENT}" ]]; then
  source ".env.${ENVIRONMENT}"
fi

# 2) Load function config
source "functions/${FUNC_DIR}/config.${ENVIRONMENT}.sh"

# 3) Common bits
: "${REGION:?missing REGION}"
: "${PROJECT_ID:?missing PROJECT_ID}"
: "${SA:?missing SA}"
: "${FUNCTION_NAME:?missing FUNCTION_NAME}"
: "${ENTRY_POINT:?missing ENTRY_POINT}"
: "${RUNTIME:=python311}"
: "${MEMORY:=256Mi}"
: "${TIMEOUT:=60s}"

CMD=( gcloud functions deploy "${FUNCTION_NAME}" 
  --project="${PROJECT_ID}"
  --gen2
  --region="${REGION}"
  --runtime="${RUNTIME}"
  --source="functions/${FUNC_DIR}"
  --entry-point="${ENTRY_POINT}"
  --service-account="${SA}"
  --memory="${MEMORY}"
  --timeout="${TIMEOUT}"
)

# 4) Trigger
case "${TRIGGER_KIND:-http}" in
  http)
    CMD+=( --trigger-http )
    # allow unauth etc.
    if [[ -n "${TRIGGER_ARGS[*]:-}" ]]; then
      CMD+=( "${TRIGGER_ARGS[@]}" )
    fi
    ;;
  bucket)
    : "${BUCKET_NAME:?missing BUCKET_NAME for bucket trigger}"
    CMD+=( --trigger-bucket="${BUCKET_NAME}" )
    if [[ -n "${TRIGGER_LOCATION:-}" ]]; then
      CMD+=( --trigger-location="${TRIGGER_LOCATION}" )
    fi
    if [[ -n "${TRIGGER_ARGS[*]:-}" ]]; then
      CMD+=( "${TRIGGER_ARGS[@]}" )
    fi
    ;;
  pubsub)
    : "${TOPIC_NAME:?missing TOPIC_NAME for pubsub trigger}"
    CMD+=( --trigger-topic="${TOPIC_NAME}" )
    if [[ -n "${TRIGGER_ARGS[*]:-}" ]]; then
      CMD+=( "${TRIGGER_ARGS[@]}" )
    fi
    ;;
  *)
    echo "Unknown TRIGGER_KIND='${TRIGGER_KIND}'"; exit 1;;
esac

# 5) Env vars
if [[ "${#ENV_VARS[@]:-0}" -gt 0 ]]; then
  IFS=, eval 'CMD+=( --set-env-vars="${ENV_VARS[*]}" )'
fi

# 6) Secrets
if [[ "${#SECRETS[@]:-0}" -gt 0 ]]; then
  IFS=, eval 'CMD+=( --set-secrets="${SECRETS[*]}" )'
fi

# 7) Optional: ingress/VPC/etc. (add here if you need)

echo "+ ${CMD[*]}"
"${CMD[@]}"
