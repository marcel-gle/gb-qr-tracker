# Prospect Enrichment Function

Cloud Function that automatically enriches business data when a new hit document is created in Firestore.

## Overview

This function:
1. Triggers on new document creation in the `hits` collection
2. Extracts business information (name, email) from the customer's business document
3. Finds personalized email using Snov.io API
4. Retrieves LinkedIn profile and job history from Snov.io
5. Generates prospect and business summaries using ChatGPT
6. Updates the business document with enriched data

## Features

- **Automatic Enrichment**: Triggers automatically on hit creation
- **Idempotent**: Skips processing if already enriched
- **API Usage Tracking**: Monitors Snov.io credits and OpenAI token usage
- **Cost Estimation**: Calculates estimated API costs
- **Comprehensive Logging**: Detailed debug logs for troubleshooting

## Setup

### 1. Create Secrets in GCP Secret Manager

```bash
# Set your project
export PROJECT_ID="gb-qr-tracker-dev"  # or "gb-qr-tracker" for prod

# Create secrets
echo -n "your-snovio-client-id" | gcloud secrets create SNOVIO_CLIENT_ID \
  --project=$PROJECT_ID \
  --data-file=-

echo -n "your-snovio-client-secret" | gcloud secrets create SNOVIO_CLIENT_SECRET \
  --project=$PROJECT_ID \
  --data-file=-

echo -n "your-openai-api-key" | gcloud secrets create OPENAI_API_KEY \
  --project=$PROJECT_ID \
  --data-file=-
```

### 2. Deploy the Function

```bash
./deploy.sh dev prospect_enrichment
```

## Local Testing

### Prerequisites

1. Install dependencies:
```bash
# Install function dependencies
cd functions/prospect_enrichment
pip install -r requirements.txt python-dotenv

# Or install from project root
pip install -r functions/prospect_enrichment/requirements.txt python-dotenv
```

2. Set up environment variables:
```bash
export PROJECT_ID="gb-qr-tracker-dev"
export DATABASE_ID="(default)"
export SNOVIO_CLIENT_ID="your-snovio-client-id"
export SNOVIO_CLIENT_SECRET="your-snovio-client-secret"
export OPENAI_API_KEY="your-openai-api-key"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

Or create a `.env` file:
```bash
# .env
PROJECT_ID=gb-qr-tracker-dev
DATABASE_ID=(default)
SNOVIO_CLIENT_ID=your-snovio-client-id
SNOVIO_CLIENT_SECRET=your-snovio-client-secret
OPENAI_API_KEY=your-openai-api-key
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

### Running the Test Script

The test script is located in the `scripts/` folder for consistency with other utility scripts.

```bash
python scripts/test_prospect_enrichment.py <owner_id> <business_id>
```

Example:
```bash
python scripts/test_prospect_enrichment.py owner123 business456
```

**Security Note:** The test script is hardcoded to use `PROJECT_ID=gb-qr-tracker-dev` to prevent accidental execution on production data.

The script will:
- Check for required environment variables
- Call the enrichment function
- Display results and API usage summary
- Show estimated costs

### Expected Output

```
============================================================
PROSPECT ENRICHMENT - LOCAL TEST
============================================================

Testing enrichment for:
  Owner ID: owner123
  Business ID: business456
  Path: customers/owner123/businesses/business456

------------------------------------------------------------
Starting enrichment process...
------------------------------------------------------------

[Function logs will appear here]

============================================================
ENRICHMENT RESULTS
============================================================

✅ Personal Email: john.doe@example.com
✅ LinkedIn URL: https://www.linkedin.com/in/johndoe/

✅ Prospect Summary (245 chars):
   John Doe is a Senior Software Engineer with 10+ years of experience...

✅ Business Summary (189 chars):
   Example Corp is a technology company specializing in...

============================================================
API USAGE SUMMARY
============================================================

Snov.io:
  Token requests: 2
  Email searches: 1 (1 credits)
  Profile requests: 1 (1 credits)

OpenAI:
  Prospect summaries: 1
  Business summaries: 1
  Total tokens: 1250
    Prompt tokens: 850
    Completion tokens: 400

Estimated Costs:
  Snov.io: 2 credits
  OpenAI: $0.1875 USD
============================================================
```

## Testing with Real Firestore Data

### Option 1: Create a Test Hit Document

Create a hit document in Firestore that will trigger the function:

```python
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP

db = firestore.Client(project="gb-qr-tracker-dev")

# Get business reference
business_ref = db.collection("businesses").document("business-id")

# Create test hit
hit_data = {
    "owner_id": "your-owner-id",
    "business_ref": business_ref,
    "link_id": "test-enrichment-trigger",
    "ts": SERVER_TIMESTAMP,
    "user_agent": "test-script",
    "device_type": "other",
    "hit_origin": "test",
}

hit_ref = db.collection("hits").document()
hit_ref.set(hit_data)
print(f"Created test hit: {hit_ref.id}")
```

### Option 2: Use Firebase Console

1. Go to Firestore Console
2. Navigate to `hits` collection
3. Create a new document with:
   - `owner_id`: Your test owner ID
   - `business_ref`: Reference to `businesses/{business_id}`
   - `link_id`: Any test link ID
   - `ts`: Server timestamp
   - `user_agent`: "test"
   - `device_type`: "other"
   - `hit_origin`: "test"

## Monitoring API Usage

### In Logs

The function automatically logs API usage at the end of each enrichment:

```
API USAGE SUMMARY
============================================================
Snov.io:
  Token requests: 2
  Email searches: 1 (1 credits)
  Profile requests: 1 (1 credits)
OpenAI:
  Prospect summaries: 1
  Business summaries: 1
  Total tokens: 1250
    Prompt tokens: 850
    Completion tokens: 400
Estimated costs:
  Snov.io: 2 credits
  OpenAI: $0.1875 USD
```

### View Function Logs

```bash
# Real-time logs
gcloud functions logs read prospect_enrichment \
  --follow \
  --project=gb-qr-tracker-dev \
  --region=europe-west3

# Recent logs
gcloud functions logs read prospect_enrichment \
  --limit=50 \
  --project=gb-qr-tracker-dev \
  --region=europe-west3

# Filter for API usage
gcloud functions logs read prospect_enrichment \
  --limit=100 \
  --project=gb-qr-tracker-dev \
  --region=europe-west3 | \
  grep -E "(API USAGE|tokens_used|credits|Snov.io|OpenAI)"
```

### Check Snov.io Credits

Visit: https://app.snov.io/account/api

### Check OpenAI Usage

Visit: https://platform.openai.com/usage

## Troubleshooting

### Function Not Triggering

- Verify Firestore trigger is configured correctly
- Check that hit document has `owner_id` and `business_ref` fields
- Verify function is deployed: `gcloud functions describe prospect_enrichment`

### No Email Found

- Check business document has `name` and `email` fields
- Verify name can be split into first/last name
- Check Snov.io has credits available
- Review logs for Snov.io API errors

### No Profile Data

- Verify personalized email was found
- Check Snov.io profile API credits
- Review logs for profile API response

### ChatGPT Failures

- Verify OpenAI API key is valid
- Check OpenAI account has credits/quota
- Review logs for API errors
- Check model name is correct (currently: `gpt-5-nano`)

### Idempotency Issues

The function checks if enrichment already exists. To re-run:
1. Remove `personal_email`, `prospect_summary`, and `business_summary` from business document
2. Or create a new hit document

## API Costs

### Snov.io
- Email search: 1 credit per search
- Profile retrieval: 1 credit per profile (verify in Snov.io docs)
- Token requests: Free (OAuth)

### OpenAI
- Model: `gpt-5-nano` (update cost constant if pricing changes)
- Current estimate: $0.15 per 1K tokens
- Typical usage per enrichment:
  - Prospect summary: ~500-1000 tokens
  - Business summary: ~300-600 tokens
  - Total: ~$0.10-0.25 per enrichment

## Configuration

Edit `config.dev.sh` or `config.prod.sh` to adjust:
- Memory: `MEMORY="512Mi"`
- Timeout: `TIMEOUT="540s"`
- Trigger resource path

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `PROJECT_ID` | GCP project ID | Yes |
| `DATABASE_ID` | Firestore database ID | Yes (default: "(default)") |
| `SNOVIO_CLIENT_ID` | Snov.io API client ID | Yes |
| `SNOVIO_CLIENT_SECRET` | Snov.io API client secret | Yes |
| `OPENAI_API_KEY` | OpenAI API key | Yes |

## Files

- `main.py`: Main function code
- `test_local.py`: Local testing script
- `config.dev.sh`: Development configuration
- `config.prod.sh`: Production configuration
- `requirements.txt`: Python dependencies

