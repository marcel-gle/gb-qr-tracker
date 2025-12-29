# Testing and Notification Setup Guide

## Step 1: Test the Function

### 1.1 Call the Function Directly

**Note:** The function requires authentication. You need to use a service account or OIDC token.

```bash
# Test the dev function (requires authentication)
# Option 1: Using gcloud auth
gcloud auth print-identity-token | xargs -I {} curl -H "Authorization: Bearer {}" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor" | python3 -m json.tool

# Option 2: Using service account (if you have credentials)
gcloud auth activate-service-account --key-file=path/to/service-account.json
gcloud auth print-identity-token | xargs -I {} curl -H "Authorization: Bearer {}" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor" | python3 -m json.tool
```

**Expected Response:**
- JSON object with `overall_success`, `health_checks`, `test_scans`, and `summary`
- If `overall_success: false`, check individual results to see what failed

### 1.2 Check Function Logs

```bash
# View recent logs
gcloud functions logs read health_monitor \
  --project=gb-qr-tracker-dev \
  --region=europe-west3 \
  --limit=20

# Or view in Console:
# https://console.cloud.google.com/functions/details/europe-west3/health_monitor?project=gb-qr-tracker-dev&tab=logs
```

### 1.3 Fix Common Issues

**Issue: Test link not found (404 errors)**
```bash
# Create the test link in Firestore
# Option 1: Via Firebase Console
# Go to: https://console.firebase.google.com/project/gb-qr-tracker-dev/firestore
# Create document: links/monitor-test-001
# With fields:
#   - destination: "https://example.com/monitor-test"
#   - active: true
#   - hit_count: 0
#   - short_code: "monitor-test-001"
#   - owner_id: "<your-owner-id>"
#   - created_at: <server timestamp>

# Option 2: Via gcloud (if you have a script)
# You'll need to create a script or use the Firebase console
```

**Issue: Domain timeout**
- Check if `ihr-brief.de` is properly configured in Cloudflare
- Verify the domain is accessible from the internet
- The timeout might be expected if the domain isn't set up yet

## Step 2: Set Up Cloud Monitoring Notifications

### 2.1 Create Email Notification Channel (Dev)

**Via Console (Recommended):**
1. Go to: https://console.cloud.google.com/monitoring/alerting/notifications?project=gb-qr-tracker-dev
2. Click **"Add New"** or **"Create Notification Channel"**
3. Select **"Email"**
4. Enter your email address
5. Display name: `Health Monitor Alerts (Dev)`
6. Click **"Save"**
7. **Verify the channel** - you'll receive a test email

**Via CLI:**
```bash
# Install alpha component if needed
gcloud components install alpha

# Create notification channel
gcloud alpha monitoring channels create \
  --display-name="Health Monitor Alerts (Dev)" \
  --type=email \
  --channel-labels=email_address=your-email@example.com \
  --project=gb-qr-tracker-dev

# Note the channel ID from output (you'll need it for the alert policy)
```

### 2.2 Create Alert Policy (Dev)

**Via Console (Recommended):**

1. Go to: https://console.cloud.google.com/monitoring/alerting/policies?project=gb-qr-tracker-dev
2. Click **"Create Policy"**
3. **Policy Details:**
   - Name: `Health Monitor Failures (Dev)`
   - Documentation: `Alerts when the health monitor detects failures in redirector endpoints or database writes in DEV environment`
4. **Add Condition:**
   - Click **"Select a metric"**
   - Search for: `Log entries` or `logging.googleapis.com/log_entry_count`
   - Resource type: `cloud_function`
   - Metric: `Log entries` (or `logging.googleapis.com/log_entry_count`)
   - **Filter** (paste exactly):
     ```
     resource.type="cloud_function"
     resource.labels.function_name="health_monitor"
     resource.labels.project_id="gb-qr-tracker-dev"
     severity="ERROR"
     textPayload=~".*HEALTH_MONITOR_FAIL.*"
     ```
   - Condition: **"Any time series violates"**
   - Threshold: `> 0`
   - Aggregation period: `1 minute`
5. **Configure Notification:**
   - Click **"Add Notification Channel"**
   - Select **"Health Monitor Alerts (Dev)"**
   - Notification delay: `0 minutes` (immediate)
6. Click **"Create Policy"**

**Alternative: Using Logs-Based Metric (More Reliable)**

1. **Create the Metric:**
   - Go to: https://console.cloud.google.com/logs/metrics?project=gb-qr-tracker-dev
   - Click **"Create Metric"**
   - Name: `health_monitor_errors_dev`
   - Description: `Count of health monitor errors in dev`
   - Type: `Counter`
   - **Filter:**
     ```
     resource.type="cloud_function"
     resource.labels.function_name="health_monitor"
     resource.labels.project_id="gb-qr-tracker-dev"
     severity="ERROR"
     textPayload=~".*HEALTH_MONITOR_FAIL.*"
     ```
   - Click **"Create Metric"**

2. **Create Alert Policy:**
   - Go to: https://console.cloud.google.com/monitoring/alerting/policies?project=gb-qr-tracker-dev
   - Click **"Create Policy"**
   - Name: `Health Monitor Failures (Dev)`
   - **Add Condition:**
     - Metric: `health_monitor_errors_dev` (search for it)
     - Condition: `> 0`
   - Add notification channel: `Health Monitor Alerts (Dev)`
   - Click **"Create Policy"**

### 2.3 Test the Alert

**Option 1: Temporarily break something**
```bash
# Update the function's TEST_LINK_ID to an invalid value
# This will cause errors and trigger the alert
# (You'd need to redeploy with a bad TEST_LINK_ID)
```

**Option 2: Check if errors are being logged**
```bash
# Check for recent errors
gcloud logging read \
  'resource.type="cloud_function" 
   resource.labels.function_name="health_monitor"
   severity="ERROR"
   textPayload=~".*HEALTH_MONITOR_FAIL.*"' \
  --project=gb-qr-tracker-dev \
  --limit=5 \
  --format=json
```

**Option 3: Manually trigger the function and check logs**
```bash
# Call the function (requires authentication)
gcloud auth print-identity-token | xargs -I {} curl -H "Authorization: Bearer {}" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor"

# Wait a minute, then check if alert fired
# Check your email for the alert notification
```

## Step 3: Set Up for Production

Once dev is working, repeat the same steps for production:

1. **Create notification channel for prod:**
   - Project: `gb-qr-tracker`
   - URL: https://console.cloud.google.com/monitoring/alerting/notifications?project=gb-qr-tracker

2. **Create alert policy for prod:**
   - Use the same filter but change `project_id` to `gb-qr-tracker`
   - URL: https://console.cloud.google.com/monitoring/alerting/policies?project=gb-qr-tracker

3. **Deploy function to prod:**
   ```bash
   ./deploy.sh prod health_monitor
   ```

## Step 4: Set Up Cloud Scheduler (Hourly Checks)

### 4.1 Create Scheduler Job for Dev

```bash
# Create hourly job for dev
gcloud scheduler jobs create http health-monitor-dev \
  --project=gb-qr-tracker-dev \
  --location=europe-west3 \
  --schedule="0 * * * *" \
  --uri="https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor" \
  --http-method=GET \
  --oidc-service-account-email=cf-campaign-importer@gb-qr-tracker-dev.iam.gserviceaccount.com \
  --time-zone="Europe/Berlin"
```

**Via Console:**
1. Go to: https://console.cloud.google.com/cloudscheduler?project=gb-qr-tracker-dev
2. Click **"Create Job"**
3. Configure:
   - Name: `health-monitor-dev`
   - Region: `europe-west3`
   - Frequency: `0 * * * *` (every hour)
   - Target: `HTTP`
   - URL: `https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor`
   - HTTP method: `GET`
   - Auth header: `Add OIDC token`
   - Service account: `cf-campaign-importer@gb-qr-tracker-dev.iam.gserviceaccount.com`

### 4.2 Test the Scheduler

```bash
# Manually trigger the job
gcloud scheduler jobs run health-monitor-dev \
  --project=gb-qr-tracker-dev \
  --location=europe-west3
```

## Quick Reference

**Function URLs:**
- Dev: `https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor`
- Prod: `https://europe-west3-gb-qr-tracker.cloudfunctions.net/health_monitor` (after deployment)

**Project IDs:**
- Dev: `gb-qr-tracker-dev`
- Prod: `gb-qr-tracker`

**Test Link ID:** `monitor-test-001` (must exist in Firestore)

**Error Pattern:** `.*HEALTH_MONITOR_FAIL.*`

## Troubleshooting

**No logs appearing:**
- Check that the function is actually being called
- Verify the function name and region are correct
- Check that logging is enabled for the function

**Alerts not firing:**
- Verify the filter matches actual log entries
- Check that the notification channel is verified
- Ensure errors are actually being logged (check severity=ERROR)

**Function returns errors:**
- Create the test link in Firestore
- Verify HMAC secret is correct
- Check that all endpoints are accessible
- Verify Firestore permissions allow read/write to `test_hits` collection

**Test Data Isolation:**
- Test hits are written to `test_hits` collection (not `hits` collection) to avoid polluting production data
- Link and campaign counters are NOT updated for test requests (this is intentional)
- Test link and campaign documents remain in normal collections (`links` and `campaigns`) but are marked with `is_test_data: true`
- Frontend should filter out test links/campaigns when displaying data (only 2 documents to filter)
- Test hits can be cleaned up independently via scripts if needed

