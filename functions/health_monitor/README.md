# Health Monitor Function

This Cloud Function monitors the health and functionality of the redirector system by:
1. Checking health endpoints for Cloudflare Worker and GCP Function (dev & prod)
2. Checking health endpoints for additional domains (e.g., ihr-brief.de, www.ihr-brief.de)
3. Performing actual test redirect scans via both paths (Worker and Direct)
4. Performing test scans for additional domains via Cloudflare Worker
5. Verifying data is correctly written to Firestore (test hits are written to `test_hits` collection)
6. Logging errors for Cloud Monitoring alerts

**Important**: Test hits are written to a separate `test_hits` collection to avoid polluting production data. Counter updates (link `hit_count`, campaign `totals.hits`, etc.) are skipped for test requests to keep production metrics accurate.

## Setup

### 1. Create Test Link in Firestore

Before deploying, create a dedicated test link in Firestore that will be used for monitoring:

**Link Document**: `links/monitor-test-001` (or your chosen TEST_LINK_ID)

Required fields:
```json
{
  "link_id": "monitor-test-001",
  "short_code": "monitor-test-001",
  "destination": "https://example.com/monitor-test",
  "active": true,
  "hit_count": 0,
  "created_at": <server_timestamp>,
  "last_hit_at": null,
  "owner_id": "<your_owner_id>"
}
```

Optional fields (for aggregate testing):
- `campaign_ref`: Reference to a campaign document
- `business_ref`: Reference to a business document
- `template_id`: Template identifier

**Create via gcloud CLI:**
```bash
# Set your project
gcloud config set project YOUR_PROJECT_ID

# Create the link document
gcloud firestore documents create \
  projects/YOUR_PROJECT_ID/databases/(default)/documents/links/monitor-test-001 \
  --data='{"destination":"https://example.com/monitor-test","active":true,"hit_count":0,"short_code":"monitor-test-001","owner_id":"YOUR_OWNER_ID"}'
```

Or create it via the Firebase Console or using a script.

### 2. Configure Environment Variables

The function uses the following environment variables (set in `config.dev.sh` and `config.prod.sh`):

- `TEST_LINK_ID`: The link ID to use for test scans (default: `monitor-test-001`)
- `CLOUDFLARE_WORKER_DEV_URL`: Cloudflare Worker dev URL (default: `https://dev.rocket-letter.de`)
- `CLOUDFLARE_WORKER_PROD_URL`: Cloudflare Worker prod URL (default: `https://go.rocket-letter.de`)
- `GCP_FUNCTION_DEV_URL`: GCP Function dev URL
- `GCP_FUNCTION_PROD_URL`: GCP Function prod URL
- `ADDITIONAL_DOMAINS`: Comma-separated list of additional domains to test (default: `ihr-brief.de,www.ihr-brief.de`)

Secrets (from Secret Manager):
- `WORKER_HMAC_SECRET`: HMAC secret for generating signatures (must match the secret used by the Cloudflare Worker)

### 3. Deploy the Function

The function is automatically deployed via GitHub Actions when changes are pushed to the `functions/health_monitor/` directory.

Manual deployment:
```bash
./deploy.sh dev health_monitor
# or
./deploy.sh prod health_monitor
```

### 4. Set Up Cloud Scheduler

Create a Cloud Scheduler job to trigger the function hourly:

**Via gcloud CLI:**
```bash
# For dev environment
gcloud scheduler jobs create http health-monitor-dev \
  --project=YOUR_PROJECT_ID \
  --location=europe-west3 \
  --schedule="0 * * * *" \
  --uri="https://europe-west3-YOUR_PROJECT_ID.cloudfunctions.net/health_monitor" \
  --http-method=GET \
  --oidc-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com

# For prod environment
gcloud scheduler jobs create http health-monitor-prod \
  --project=YOUR_PROJECT_ID \
  --location=europe-west3 \
  --schedule="0 * * * *" \
  --uri="https://europe-west3-YOUR_PROJECT_ID.cloudfunctions.net/health_monitor" \
  --http-method=GET \
  --oidc-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

**Via Console:**
1. Go to Cloud Scheduler in GCP Console
2. Click "Create Job"
3. Configure:
   - Name: `health-monitor-dev` (or `health-monitor-prod`)
   - Region: `europe-west3`
   - Frequency: `0 * * * *` (every hour)
   - Target type: HTTP
   - URL: Your function URL
   - HTTP method: GET
   - Auth header: Add OIDC token
   - Service account: Your service account with Cloud Functions Invoker role

### 5. Set Up Cloud Monitoring Alert Policy

The health monitor logs errors with the prefix `[HEALTH_MONITOR_FAIL]` when any check fails. To receive email notifications, you need to:

#### Step 1: Create a Notification Channel (Email)

**Via Console:**
1. Go to [Cloud Monitoring > Alerting > Notification Channels](https://console.cloud.google.com/monitoring/alerting/notifications)
2. Click "Add New" or "Create Notification Channel"
3. Select "Email"
4. Enter your email address
5. Enter a display name (e.g., "Health Monitor Alerts")
6. Click "Save"

**Via gcloud CLI:**
```bash
# Create email notification channel
gcloud alpha monitoring channels create \
  --display-name="Health Monitor Alerts" \
  --type=email \
  --channel-labels=email_address=your-email@example.com \
  --project=YOUR_PROJECT_ID

# Note the channel ID from the output - you'll need it for the alert policy
```

#### Step 2: Create an Alert Policy

**Via Console (Recommended):**
1. Go to [Cloud Monitoring > Alerting > Policies](https://console.cloud.google.com/monitoring/alerting/policies)
2. Click "Create Policy"
3. **Policy Details:**
   - Name: "Health Monitor Failures"
   - Documentation: "Alerts when the health monitor detects failures in redirector endpoints or database writes"
4. **Add Condition:**
   - Click "Select a metric"
   - Choose "Logs-based metric" or search for "Log entries"
   - Resource type: `cloud_function`
   - Metric: `logging.googleapis.com/log_entry_count` or `Log entries`
   - Filter (paste this exactly):
     ```
     resource.type="cloud_function"
     resource.labels.function_name="health_monitor"
     severity="ERROR"
     textPayload=~".*HEALTH_MONITOR_FAIL.*"
     ```
   - Condition: "Any time series violates"
   - Threshold: `> 0` (any error triggers alert)
   - Advanced Options: Aggregation period: 1 minute
5. **Configure Notification:**
   - Click "Add Notification Channel"
   - Select the email channel you created in Step 1
   - Notification delay: 0 minutes (immediate notification)
6. Click "Create Policy"

**Alternative: Using Logs-Based Metric (More Reliable)**

1. Go to [Cloud Monitoring > Logs > Logs-based Metrics](https://console.cloud.google.com/logs/metrics)
2. Click "Create Metric"
3. **Metric Details:**
   - Name: `health_monitor_errors`
   - Description: "Count of health monitor errors"
   - Type: Counter
4. **Filter:**
   ```
   resource.type="cloud_function"
   resource.labels.function_name="health_monitor"
   severity="ERROR"
   textPayload=~".*HEALTH_MONITOR_FAIL.*"
   ```
5. Click "Create Metric"
6. **Create Alert Policy:**
   - Go to Alerting > Policies > Create Policy
   - Metric: `health_monitor_errors`
   - Condition: `> 0`
   - Add your email notification channel

**Via gcloud CLI (Advanced):**

```bash
# First, get your notification channel ID
CHANNEL_ID=$(gcloud alpha monitoring channels list \
  --filter="displayName='Health Monitor Alerts'" \
  --format="value(name)" \
  --project=YOUR_PROJECT_ID)

# Create alert policy (requires JSON configuration)
# This is complex - using Console is recommended
```

#### Step 3: Test the Alert

To verify notifications work:
1. Manually trigger a failure (e.g., temporarily use an invalid TEST_LINK_ID)
2. Wait for the next scheduled run (or trigger manually)
3. You should receive an email within a few minutes

**Manual Test:**
```bash
# Call the function to see current status
curl "https://europe-west3-YOUR_PROJECT_ID.cloudfunctions.net/health_monitor"

# Check logs to see if errors are being logged
gcloud functions logs read health_monitor \
  --limit=50 \
  --project=YOUR_PROJECT_ID
```

## Testing

Test the function manually:

**Note:** The function requires authentication for security. Use one of these methods:

```bash
# Option 1: Using gcloud identity token
gcloud auth print-identity-token | xargs -I {} curl -H "Authorization: Bearer {}" \
  "https://europe-west3-YOUR_PROJECT_ID.cloudfunctions.net/health_monitor"

# Option 2: Using service account
gcloud auth activate-service-account --key-file=path/to/service-account.json
gcloud auth print-identity-token | xargs -I {} curl -H "Authorization: Bearer {}" \
  "https://europe-west3-YOUR_PROJECT_ID.cloudfunctions.net/health_monitor"
```

The response will be a JSON object with:
- `overall_success`: Boolean indicating if all checks passed
- `health_checks`: Results for each health endpoint
- `test_scans`: Results for each test scan path
- `summary`: Summary of all checks

## Monitoring

The function logs the following:
- **INFO**: Successful checks and overall status
- **WARNING**: Partial failures (some checks passed, some failed)
- **ERROR**: Complete failures or exceptions (these trigger Cloud Monitoring alerts)

Error logs include the component, check type, and error details for easy debugging.

## Troubleshooting

### Test link not found
- Ensure the test link exists in Firestore with the correct ID
- Check that `TEST_LINK_ID` environment variable matches the link ID

### HMAC signature failures
- Verify `WORKER_HMAC_SECRET` matches the secret used by the Cloudflare Worker
- Check that the secret is correctly set in Secret Manager

### Database verification failures
- Ensure the test link is active (`active: true`)
- Check that Firestore permissions allow the function to read/write to `test_hits` collection
- Verify the hit was actually created in `test_hits` collection (may take a few seconds)
- Note: Test hits are written to `test_hits` collection, not `hits` collection
- Note: Link counters are not updated for test requests (this is intentional to avoid polluting production metrics)

### Timeout errors
- Increase `REQUEST_TIMEOUT` in the code if network is slow
- Check that all endpoints are accessible from the Cloud Function

