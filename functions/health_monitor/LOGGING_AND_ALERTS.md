# Logging and Alert Setup Guide

## What Logs Are Generated

The health monitor function generates logs at different severity levels:

### 1. INFO Logs (Normal Operation)

**When:** Every time the function runs successfully
- `"Starting health monitor check"` - Function execution begins
- `"Health monitor check passed"` - All checks passed successfully

**Example:**
```
INFO: Starting health monitor check
INFO: Health monitor check passed
```

### 2. WARNING Logs (Partial Failures)

**When:** Some checks fail but function completes
- `"Health monitor check failed"` - Includes full result object with details

**Example:**
```
WARNING: Health monitor check failed {"result": {"overall_success": false, ...}}
```

### 3. ERROR Logs (Failures - These Trigger Alerts)

**When:** Individual checks fail or exceptions occur

**Format:** All error logs include the prefix `[HEALTH_MONITOR_FAIL]` for easy filtering

**Error Types:**

1. **Health Check Failures:**
   ```
   ERROR: [HEALTH_MONITOR_FAIL] cloudflare_worker - health_check: Request timeout | Details: {"environment": "dev"}
   ERROR: [HEALTH_MONITOR_FAIL] gcp_function - health_check: Expected 200 with 'ok' body, got 500 | Details: {"environment": "prod"}
   ```

2. **Test Scan Failures:**
   ```
   ERROR: [HEALTH_MONITOR_FAIL] cloudflare_worker - test_scan: Expected redirect (301/302), got 404 | Details: {"environment": "dev", "link_id": "monitor-test-001"}
   ERROR: [HEALTH_MONITOR_FAIL] gcp_function - test_scan: Request timeout | Details: {"environment": "prod", "link_id": "monitor-test-001", "path": "direct"}
   ```

3. **Database Verification Failures:**
   ```
   ERROR: [HEALTH_MONITOR_FAIL] cloudflare_worker - db_verification: No matching hit found in database | Details: {"environment": "dev", "link_id": "monitor-test-001"}
   ERROR: [HEALTH_MONITOR_FAIL] gcp_function - db_verification: Link hit_count is 0, expected > 0 | Details: {"environment": "prod", "link_id": "monitor-test-001", "path": "direct"}
   ```

4. **Exceptions:**
   ```
   ERROR: [HEALTH_MONITOR_FAIL] health_monitor - exception: Health monitor check failed with exception: ...
   ```

**Error Log Structure:**
- Component: `cloudflare_worker`, `gcp_function`, or `health_monitor`
- Check Type: `health_check`, `test_scan`, `db_verification`, or `exception`
- Error Message: Descriptive error text
- Details: JSON object with context (environment, link_id, domain, etc.)

## Viewing Logs

### Via Console:
1. Go to: https://console.cloud.google.com/functions/details/europe-west3/health_monitor?project=gb-qr-tracker-dev&tab=logs
2. Filter by severity: ERROR, WARNING, or INFO
3. Search for: `HEALTH_MONITOR_FAIL` to see all failures

### Via CLI:
```bash
# View all logs
gcloud functions logs read health_monitor \
  --project=gb-qr-tracker-dev \
  --region=europe-west3 \
  --limit=50

# View only ERROR logs
gcloud logging read \
  'resource.type="cloud_function"
   resource.labels.function_name="health_monitor"
   severity="ERROR"' \
  --project=gb-qr-tracker-dev \
  --limit=20 \
  --format=json

# View logs with HEALTH_MONITOR_FAIL prefix
gcloud logging read \
  'resource.type="cloud_function"
   resource.labels.function_name="health_monitor"
   textPayload=~".*HEALTH_MONITOR_FAIL.*"' \
  --project=gb-qr-tracker-dev \
  --limit=20 \
  --format=json
```

## Setting Up Alerts

### Step 1: Create Email Notification Channel

**Via Console:**
1. Go to: https://console.cloud.google.com/monitoring/alerting/notifications?project=gb-qr-tracker-dev
2. Click **"Add New"** or **"Create Notification Channel"**
3. Select **"Email"**
4. Enter your email address
5. Display name: `Health Monitor Alerts (Dev)`
6. Click **"Save"**
7. **Verify the channel** - check your email and click the verification link

**Via CLI:**
```bash
gcloud alpha monitoring channels create \
  --display-name="Health Monitor Alerts (Dev)" \
  --type=email \
  --channel-labels=email_address=your-email@example.com \
  --project=gb-qr-tracker-dev
```

### Step 2: Create Logs-Based Metric (Recommended)

This approach is more reliable than direct log filtering:

1. **Go to Logs-Based Metrics:**
   - https://console.cloud.google.com/logs/metrics?project=gb-qr-tracker-dev

2. **Click "Create Metric"**

3. **Metric Details:**
   - Name: `health_monitor_errors_dev`
   - Description: `Count of health monitor errors in dev environment`
   - Type: `Counter`

4. **Filter (paste exactly):**
   ```
   resource.type="cloud_function"
   resource.labels.function_name="health_monitor"
   resource.labels.project_id="gb-qr-tracker-dev"
   severity="ERROR"
   textPayload=~".*HEALTH_MONITOR_FAIL.*"
   ```

5. **Click "Create Metric"**

### Step 3: Create Alert Policy

**Via Console:**

1. **Go to Alert Policies:**
   - https://console.cloud.google.com/monitoring/alerting/policies?project=gb-qr-tracker-dev

2. **Click "Create Policy"**

3. **Policy Details:**
   - Name: `Health Monitor Failures (Dev)`
   - Documentation: 
     ```
     Alerts when the health monitor detects failures in redirector endpoints or database writes in DEV environment.
     
     This alert triggers when:
     - Health endpoint checks fail
     - Test redirect scans fail
     - Database verification fails
     - Exceptions occur during monitoring
     ```

4. **Add Condition:**
   - Click **"Select a metric"**
   - Search for: `health_monitor_errors_dev` (the metric you just created)
   - Or use: `Log entries` with the filter below
   
   **If using Log entries directly:**
   - Resource type: `cloud_function`
   - Metric: `Log entries` or `logging.googleapis.com/log_entry_count`
   - **Filter:**
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
   - You can add multiple channels (email, Slack, PagerDuty, etc.)

6. **Click "Create Policy"**

### Step 4: Test the Alert

**Option 1: Check Recent Errors**
```bash
# Check if there are any recent errors
gcloud logging read \
  'resource.type="cloud_function"
   resource.labels.function_name="health_monitor"
   resource.labels.project_id="gb-qr-tracker-dev"
   severity="ERROR"
   textPayload=~".*HEALTH_MONITOR_FAIL.*"' \
  --project=gb-qr-tracker-dev \
  --limit=5 \
  --format=json
```

**Option 2: Temporarily Break Something**
- Temporarily set `TEST_LINK_ID` to an invalid value in the function
- Redeploy
- Wait for the next scheduled run
- You should receive an alert email

**Option 3: Manually Trigger**
```bash
# Manually trigger the function (will create logs)
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor"
```

## Alert Filter Examples

### All Errors:
```
resource.type="cloud_function"
resource.labels.function_name="health_monitor"
resource.labels.project_id="gb-qr-tracker-dev"
severity="ERROR"
textPayload=~".*HEALTH_MONITOR_FAIL.*"
```

### Only Health Check Failures:
```
resource.type="cloud_function"
resource.labels.function_name="health_monitor"
resource.labels.project_id="gb-qr-tracker-dev"
severity="ERROR"
textPayload=~".*HEALTH_MONITOR_FAIL.*"
textPayload=~".*health_check.*"
```

### Only Database Verification Failures:
```
resource.type="cloud_function"
resource.labels.function_name="health_monitor"
resource.labels.project_id="gb-qr-tracker-dev"
severity="ERROR"
textPayload=~".*HEALTH_MONITOR_FAIL.*"
textPayload=~".*db_verification.*"
```

### Only Cloudflare Worker Failures:
```
resource.type="cloud_function"
resource.labels.function_name="health_monitor"
resource.labels.project_id="gb-qr-tracker-dev"
severity="ERROR"
textPayload=~".*HEALTH_MONITOR_FAIL.*"
textPayload=~".*cloudflare_worker.*"
```

## Production Setup

Repeat the same steps for production:

1. **Notification Channel:**
   - Project: `gb-qr-tracker`
   - URL: https://console.cloud.google.com/monitoring/alerting/notifications?project=gb-qr-tracker

2. **Logs-Based Metric:**
   - Name: `health_monitor_errors_prod`
   - Filter: Change `project_id` to `gb-qr-tracker`

3. **Alert Policy:**
   - Name: `Health Monitor Failures (Prod)`
   - Use the same filter but with `project_id="gb-qr-tracker"`

## What You'll Receive in Alerts

When an alert fires, you'll receive an email with:
- **Alert Name:** Health Monitor Failures (Dev)
- **Condition:** Details about which metric triggered
- **Resource:** The Cloud Function name and project
- **Summary:** Number of errors detected
- **Details:** Link to view logs and metrics

You can click through to see the actual log entries with full error messages and context.

## Best Practices

1. **Set up separate alerts for dev and prod** - Different severity levels
2. **Use logs-based metrics** - More reliable than direct log filtering
3. **Test your alerts** - Verify they work before relying on them
4. **Monitor alert frequency** - Too many false positives? Adjust thresholds
5. **Add multiple notification channels** - Email + Slack/PagerDuty for critical alerts


