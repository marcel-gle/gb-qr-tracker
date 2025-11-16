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

# Ensure optional arrays/vars exist (avoids unset errors with `set -u`)
declare -a ENV_VARS SECRETS TRIGGER_ARGS
: "${TRIGGER_KIND:=http}"

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
case "${TRIGGER_KIND}" in
  http)
    CMD+=( --trigger-http )
    # extra flags (e.g. --allow-unauthenticated)
    if ((${#TRIGGER_ARGS[@]})); then
      CMD+=( "${TRIGGER_ARGS[@]}" )
    fi
    ;;
  bucket)
    : "${BUCKET_NAME:?missing BUCKET_NAME for bucket trigger}"
    CMD+=( --trigger-bucket="${BUCKET_NAME}" )
    if [[ -n "${TRIGGER_LOCATION:-}" ]]; then
      CMD+=( --trigger-location="${TRIGGER_LOCATION}" )
    fi
    if ((${#TRIGGER_ARGS[@]})); then
      CMD+=( "${TRIGGER_ARGS[@]}" )
    fi
    ;;
  pubsub)
    : "${TOPIC_NAME:?missing TOPIC_NAME for pubsub trigger}"
    CMD+=( --trigger-topic="${TOPIC_NAME}" )
    if ((${#TRIGGER_ARGS[@]})); then
      CMD+=( "${TRIGGER_ARGS[@]}" )
    fi
    ;;
  *)
    echo "Unknown TRIGGER_KIND='${TRIGGER_KIND}'"; exit 1;;
esac

# 5) Env vars
if ((${#ENV_VARS[@]})); then
  # Check if any env var value contains a comma (which breaks gcloud parsing)
  needs_file=false
  for var in "${ENV_VARS[@]}"; do
    # Extract value part (after =)
    if [[ "$var" =~ = ]] && [[ "$var" =~ , ]]; then
      needs_file=true
      break
    fi
  done
  
  if [[ "$needs_file" == "true" ]]; then
    # Use env-vars-file for values with commas (YAML format)
    env_file=$(mktemp)
    for var in "${ENV_VARS[@]}"; do
      # Split key and value
      if [[ "$var" =~ ^([^=]+)=(.*)$ ]]; then
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
        # Remove quotes from value if present
        value="${value#\"}"
        value="${value%\"}"
        value="${value#\'}"
        value="${value%\'}"
        # Write in YAML format (key: value)
        # Escape colons in value if present
        echo "${key}: ${value}" >> "$env_file"
      fi
    done
    CMD+=( --env-vars-file="${env_file}" )
    # Clean up temp file after deployment (using trap)
    trap "rm -f ${env_file}" EXIT
  else
    # Use direct flag for simple values
    env_join=$(IFS=,; printf "%s" "${ENV_VARS[*]}")
    CMD+=( --set-env-vars="${env_join}" )
  fi
fi

# 6) Secrets
if ((${#SECRETS[@]})); then
  secrets_join=$(IFS=,; printf "%s" "${SECRETS[*]}")
  CMD+=( --set-secrets="${secrets_join}" )
fi

# 7) Optional: ingress/VPC/etc. (add here if you need)

echo "+ ${CMD[*]}"
"${CMD[@]}"
