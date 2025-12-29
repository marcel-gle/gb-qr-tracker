# Deployment Guide

This guide covers deploying GB QR Tracker to Google Cloud Platform and Cloudflare.

## Prerequisites

- Google Cloud SDK (`gcloud`) installed and configured
- Cloudflare account with Workers access
- Node.js and npm (for Cloudflare Worker)
- Python 3.11+ (for local testing)
- Service account with appropriate permissions

## Initial Setup

### 1. Create GCP Project

```bash
# Create project (or use existing)
gcloud projects create gb-qr-tracker-dev --name="GB QR Tracker Dev"

# Set as active project
gcloud config set project gb-qr-tracker-dev
```

### 2. Enable Required APIs

```bash
gcloud services enable \
  cloudfunctions.googleapis.com \
  firestore.googleapis.com \
  storage.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com
```

### 3. Create Service Account

```bash
# Create service account
gcloud iam service-accounts create gb-qr-tracker-sa \
  --display-name="GB QR Tracker Service Account"

# Grant required roles
gcloud projects add-iam-policy-binding gb-qr-tracker-dev \
  --member="serviceAccount:gb-qr-tracker-sa@gb-qr-tracker-dev.iam.gserviceaccount.com" \
  --role="roles/datastore.user"

gcloud projects add-iam-policy-binding gb-qr-tracker-dev \
  --member="serviceAccount:gb-qr-tracker-sa@gb-qr-tracker-dev.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding gb-qr-tracker-dev \
  --member="serviceAccount:gb-qr-tracker-sa@gb-qr-tracker-dev.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### 4. Create Firestore Database

```bash
# Create Firestore database (Native mode)
gcloud firestore databases create --region=europe-west3
```

### 5. Create Storage Bucket

```bash
# Create bucket for uploads
gsutil mb -p gb-qr-tracker-dev -l eu gs://gb-qr-tracker-dev.firebasestorage.app

# Set CORS (if needed)
gsutil cors set cors.json gs://gb-qr-tracker-dev.firebasestorage.app
```

### 6. Create Secrets

```bash
# IP hash salt
echo -n "your-random-salt-here" | gcloud secrets create IP_HASH_SALT \
  --data-file=- \
  --project=gb-qr-tracker-dev

# Worker HMAC secret
echo -n "your-hmac-secret-here" | gcloud secrets create WORKER_HMAC_SECRET \
  --data-file=- \
  --project=gb-qr-tracker-dev

# Mapbox token (optional)
echo -n "your-mapbox-token" | gcloud secrets create MAPBOX_TOKEN \
  --data-file=- \
  --project=gb-qr-tracker-dev
```

### 7. Deploy Firestore Indexes

```bash
# Import composite indexes
gcloud firestore indexes import firestore_indexes/composite-indexes.json \
  --project=gb-qr-tracker-dev

# Wait for indexes to build (check in console)
gcloud firestore indexes list --project=gb-qr-tracker-dev
```

## Environment Configuration

### Create Environment Files

Create `.env.dev` and `.env.prod` in the repository root:

**`.env.dev`**:
```bash
export PROJECT_ID="gb-qr-tracker-dev"
export REGION="europe-west3"
export SA="gb-qr-tracker-sa@gb-qr-tracker-dev.iam.gserviceaccount.com"
```

**`.env.prod`**:
```bash
export PROJECT_ID="gb-qr-tracker"
export REGION="europe-west3"
export SA="gb-qr-tracker-sa@gb-qr-tracker.iam.gserviceaccount.com"
```

## Deploying Cloud Functions

### Using the Deploy Script

The repository includes a deployment script (`deploy.sh`) that simplifies function deployment.

**Syntax**:
```bash
./deploy.sh <environment> <function_directory>
```

**Example**:
```bash
# Deploy redirector to dev
./deploy.sh dev redirector

# Deploy upload_processor to prod
./deploy.sh prod upload_processor
```

### Function-Specific Deployment

#### 1. Redirector Function

**Config**: `functions/redirector/config.dev.sh` or `config.prod.sh`

**Deploy**:
```bash
./deploy.sh dev redirector
```

**Manual Deploy** (if needed):
```bash
source .env.dev
source functions/redirector/config.dev.sh

gcloud functions deploy redirector \
  --project=$PROJECT_ID \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=functions/redirector \
  --entry-point=redirector \
  --service-account=$SA \
  --trigger-http \
  --allow-unauthenticated \
  --memory=256Mi \
  --timeout=60s \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,DATABASE_ID="(default)",HIT_TTL_DAYS=30,GEOIP_API_URL=https://ipapi.co/{ip}/json/,STORE_IP_HASH=1,LOG_HIT_ERRORS=1 \
  --set-secrets=IP_HASH_SALT=projects/$PROJECT_ID/secrets/IP_HASH_SALT:latest,WORKER_HMAC_SECRET=projects/$PROJECT_ID/secrets/WORKER_HMAC_SECRET:latest
```

#### 2. Upload Processor Function

**Config**: `functions/upload_processor/config.dev.sh` or `config.prod.sh`

**Deploy**:
```bash
./deploy.sh dev upload_processor
```

**Manual Deploy**:
```bash
source .env.dev
source functions/upload_processor/config.dev.sh

gcloud functions deploy upload_processor \
  --project=$PROJECT_ID \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=functions/upload_processor \
  --entry-point=process_business_upload \
  --service-account=$SA \
  --trigger-bucket=$BUCKET_NAME \
  --trigger-location=eu \
  --memory=1GiB \
  --timeout=540s \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,DATABASE_ID="(default)" \
  --set-secrets=MAPBOX_TOKEN=projects/$PROJECT_ID/secrets/MAPBOX_TOKEN:latest
```

#### 3. Health Monitor Function

**Config**: `functions/health_monitor/config.dev.sh` or `config.prod.sh`

**Deploy**:
```bash
./deploy.sh dev health_monitor
```

**Setup Cloud Scheduler** (after deployment):
```bash
# Get function URL
FUNCTION_URL=$(gcloud functions describe health_monitor \
  --gen2 \
  --region=$REGION \
  --project=$PROJECT_ID \
  --format="value(serviceConfig.uri)")

# Create scheduler job
gcloud scheduler jobs create http health-monitor-dev \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="0 * * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=GET \
  --oidc-service-account-email=$SA \
  --time-zone="Europe/Berlin"
```

#### 4. Create Customer Function

**Deploy**:
```bash
./deploy.sh dev create_customer
```

#### 5. List Campaign Files Function

**Deploy**:
```bash
./deploy.sh dev list_campaign_files
```

#### 6. Delete Campaign Function

**Deploy**:
```bash
./deploy.sh dev delete_campaign
```

## Deploying Cloudflare Worker

### 1. Install Dependencies

```bash
cd workers/redirector
npm install
```

### 2. Configure Wrangler

Edit `wrangler.jsonc`:
- Set `BACKEND_FUNCTION_URL` to your GCP Function URL
- Configure routes for your domains

### 3. Set Secrets

```bash
# Set HMAC secret (must match GCP secret)
npx wrangler secret put WORKER_HMAC_SECRET
# Enter the same secret value as in GCP Secret Manager
```

### 4. Deploy

**Development**:
```bash
npm run deploy
# or
npx wrangler deploy
```

**Production**:
```bash
npx wrangler deploy --env production
```

### 5. Verify Deployment

```bash
# Test health endpoint
curl https://dev.rocket-letter.de/health
# Should return: ok

# Test redirect
curl -v https://dev.rocket-letter.de/test-link-id
# Should redirect to destination
```

## Post-Deployment Setup

### 1. Create Test Link

Create a test link in Firestore for health monitoring:

```bash
gcloud firestore documents create \
  projects/gb-qr-tracker-dev/databases/\(default\)/documents/links/monitor-test-001 \
  --data='{
    "destination": "https://example.com/monitor-test",
    "active": true,
    "hit_count": 0,
    "short_code": "monitor-test-001",
    "owner_id": "test-owner-id"
  }'
```

### 2. Configure Firestore TTL (Optional)

If using `HIT_TTL_DAYS`:

1. Go to Firestore Console
2. Select `hits` collection
3. Enable TTL on `expires_at` field
4. Set policy to delete expired documents

### 3. Set Up Cloud Monitoring Alerts

**Create Alert Policy**:

1. Go to Cloud Monitoring > Alerting > Policies
2. Click "Create Policy"
3. Add condition:
   - Metric: `logging.googleapis.com/log_entry_count`
   - Filter: `resource.type="cloud_function" AND severity="ERROR" AND textPayload=~".*HEALTH_MONITOR_FAIL.*"`
   - Threshold: `> 0`
4. Add notification channel (email)
5. Save policy

### 4. Test End-to-End

```bash
# 1. Test redirector directly
curl -v "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/redirector?id=monitor-test-001"

# 2. Test via Cloudflare Worker
curl -v "https://dev.rocket-letter.de/monitor-test-001"

# 3. Test health monitor
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor"

# 4. Test upload (upload a test CSV)
gsutil cp test.csv gs://gb-qr-tracker-dev.firebasestorage.app/uploads/dev/test-uid/test-campaign/source/
```

## Production Deployment Checklist

- [ ] All functions deployed to production
- [ ] Cloudflare Worker deployed to production
- [ ] Secrets created in production project
- [ ] Firestore indexes deployed
- [ ] Test link created
- [ ] Health monitor scheduled
- [ ] Cloud Monitoring alerts configured
- [ ] CORS configured (if needed)
- [ ] Domain DNS configured
- [ ] SSL certificates valid
- [ ] Backup strategy in place
- [ ] Monitoring dashboards created

## Updating Deployments

### Update a Function

```bash
# Make code changes
# Then redeploy
./deploy.sh dev redirector
```

### Update Cloudflare Worker

```bash
cd workers/redirector
# Make code changes
npm run deploy
```

### Update Secrets

```bash
# Update secret value
echo -n "new-secret-value" | gcloud secrets versions add SECRET_NAME \
  --data-file=- \
  --project=$PROJECT_ID

# Functions automatically use latest version (if configured with :latest)
# Or restart function to pick up new version
```

## Rollback Procedures

### Rollback Function

```bash
# List versions
gcloud functions versions list redirector \
  --gen2 \
  --region=$REGION \
  --project=$PROJECT_ID

# Rollback to previous version
gcloud functions deploy redirector \
  --gen2 \
  --region=$REGION \
  --project=$PROJECT_ID \
  --source=functions/redirector \
  # ... other flags
```

### Rollback Cloudflare Worker

```bash
# Deploy previous version
cd workers/redirector
git checkout previous-commit
npm run deploy
```

## Troubleshooting Deployment

### Function Deployment Fails

**Check**:
- Service account permissions
- API enablement
- Resource quotas
- Function code syntax

**Debug**:
```bash
# View build logs
gcloud builds list --project=$PROJECT_ID

# View function logs
gcloud functions logs read redirector --limit=50
```

### Cloudflare Worker Deployment Fails

**Check**:
- Wrangler authentication
- Route configuration
- Secret values

**Debug**:
```bash
# Test locally
npm run dev

# View logs
npx wrangler tail
```

### Firestore Index Build Fails

**Check**:
- Index definition syntax
- Field names match schema
- Collection names correct

**Debug**:
```bash
# List indexes
gcloud firestore indexes list

# View index status
gcloud firestore indexes describe INDEX_ID
```

## Environment-Specific Notes

### Development Environment

- Uses `gb-qr-tracker-dev` project
- Test database: `(default)` or `test`
- Lower resource limits
- More verbose logging

### Production Environment

- Uses `gb-qr-tracker` project
- Production database: `(default)`
- Higher resource limits
- Optimized logging
- Monitoring alerts enabled

## Security Considerations

### Secrets Management

- Never commit secrets to repository
- Use Secret Manager for sensitive values
- Rotate secrets regularly
- Use least-privilege service accounts

### Network Security

- Functions use private networking (VPC) if needed
- Cloudflare Worker adds DDoS protection
- HTTPS only (no HTTP)

### Access Control

- Firestore security rules enforce ownership
- Functions verify authentication
- Admin functions require `isAdmin` claim

## Cost Optimization

### Firestore

- Use TTL for automatic cleanup
- Index only queried fields
- Batch reads/writes when possible

### Cloud Functions

- Right-size memory allocation
- Use appropriate timeouts
- Monitor invocation counts

### Cloudflare Worker

- Free tier: 100k requests/day
- Paid tier: $5/month for 10M requests

## Monitoring & Maintenance

### Regular Tasks

- Review Cloud Monitoring dashboards weekly
- Check health monitor results daily
- Review function logs for errors
- Monitor Firestore costs
- Update dependencies monthly

### Maintenance Windows

- Schedule during low-traffic periods
- Notify users of planned downtime
- Test in dev before prod
- Have rollback plan ready

