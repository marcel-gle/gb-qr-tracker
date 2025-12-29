# Development Guide

Guide for developers working on GB QR Tracker.

## Table of Contents

1. [Local Development Setup](#local-development-setup)
2. [Code Structure](#code-structure)
3. [Development Workflow](#development-workflow)
4. [Testing](#testing)
5. [Code Patterns](#code-patterns)
6. [Debugging](#debugging)
7. [Contributing](#contributing)

## Local Development Setup

### Prerequisites

- Python 3.11+
- Node.js 18+ and npm
- Google Cloud SDK (`gcloud`)
- Service account JSON key file
- Firebase CLI (optional, for emulators)

### Initial Setup

1. **Clone Repository**:
   ```bash
   git clone <repository-url>
   cd gb-qr-tracker
   ```

2. **Install Python Dependencies**:
   ```bash
   # Install for each function
   pip install -r functions/redirector/requirements.txt
   pip install -r functions/upload_processor/requirements.txt
   pip install -r functions/health_monitor/requirements.txt
   pip install -r functions/create_customer/requirements.txt
   pip install -r functions/list_campaign_files/requirements.txt
   pip install -r functions/delete_campaign/requirements.txt
   ```

3. **Install Node.js Dependencies**:
   ```bash
   cd workers/redirector
   npm install
   cd ../..
   ```

4. **Set Up Service Account**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
   ```

5. **Set Environment Variables**:
   ```bash
   export PROJECT_ID="gb-qr-tracker-dev"
   export DATABASE_ID="(default)"
   ```

### Running Functions Locally

#### Redirector Function

```bash
cd functions/redirector
functions-framework --target=redirector --port=8080 --debug
```

Test:
```bash
curl "http://localhost:8080?id=test-link-id"
```

#### Upload Processor Function

The upload processor is triggered by Cloud Storage events, so local testing requires:
- GCS bucket with test file
- Cloud Functions emulator (Gen 2 not fully supported)
- Or deploy to dev and test there

#### Health Monitor Function

```bash
cd functions/health_monitor
functions-framework --target=health_monitor --port=8080 --debug
```

Test:
```bash
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "http://localhost:8080"
```

### Running Cloudflare Worker Locally

```bash
cd workers/redirector
npm run dev
```

Worker runs on `http://localhost:8787`

### Using Firestore Emulator

```bash
# Install emulator
gcloud components install cloud-firestore-emulator

# Start emulator
gcloud emulators firestore start --host-port=localhost:8080

# Set environment
export FIRESTORE_EMULATOR_HOST="localhost:8080"

# Run functions (they'll use emulator)
```

## Code Structure

### Directory Layout

```
gb-qr-tracker/
├── functions/              # Google Cloud Functions
│   ├── redirector/        # Redirect handler
│   ├── upload_processor/  # CSV/XLSX processor
│   ├── health_monitor/    # Health monitoring
│   ├── create_customer/   # Customer management
│   ├── list_campaign_files/ # File listing
│   └── delete_campaign/   # Campaign deletion
├── workers/               # Cloudflare Workers
│   └── redirector/        # Edge routing
├── scripts/               # Utility scripts
│   ├── migrate_*.py       # Migration scripts
│   ├── normalize_*.py    # Data normalization
│   └── *.py               # Other utilities
├── firestore_indexes/     # Firestore index definitions
├── data/                  # Sample/test data
└── deploy.sh              # Deployment script
```

### Function Structure

Each function follows this structure:

```
function_name/
├── main.py                # Main entry point
├── requirements.txt       # Python dependencies
├── config.dev.sh         # Dev environment config
└── config.prod.sh        # Prod environment config
```

### Key Files

**`main.py`**: Function entry point
- Uses `@functions_framework.http` or `@functions_framework.cloud_event`
- Contains handler function
- Imports utilities and helpers

**`config.{env}.sh`**: Environment configuration
- `FUNCTION_NAME`: Function name
- `ENTRY_POINT`: Python function name
- `RUNTIME`: Python version
- `MEMORY`: Memory allocation
- `TIMEOUT`: Function timeout
- `TRIGGER_KIND`: Trigger type (http, bucket, pubsub)
- `ENV_VARS`: Environment variables array
- `SECRETS`: Secret Manager references

## Development Workflow

### Making Changes

1. **Create Feature Branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes**:
   - Edit function code
   - Update tests
   - Update documentation

3. **Test Locally**:
   - Run function locally
   - Test with sample data
   - Verify Firestore operations

4. **Deploy to Dev**:
   ```bash
   ./deploy.sh dev function_name
   ```

5. **Test in Dev**:
   - Verify function works
   - Check logs
   - Test edge cases

6. **Deploy to Prod** (after review):
   ```bash
   ./deploy.sh prod function_name
   ```

### Code Review Checklist

- [ ] Code follows style guidelines
- [ ] Error handling is appropriate
- [ ] Logging is sufficient
- [ ] No hardcoded values
- [ ] Secrets use Secret Manager
- [ ] Firestore queries are efficient
- [ ] Tests pass
- [ ] Documentation updated

### Commit Messages

Follow conventional commits:
```
feat: add geocoding support to upload processor
fix: handle empty link IDs in redirector
docs: update API documentation
refactor: simplify ID generation logic
```

## Testing

### Unit Testing

Create test files: `test_{function_name}.py`

**Example**:
```python
import unittest
from functions.redirector.main import _extract_link_id
from flask import Request

class TestRedirector(unittest.TestCase):
    def test_extract_link_id_from_query(self):
        request = Request.from_values(query_string='id=test-123')
        self.assertEqual(_extract_link_id(request), 'test-123')
```

Run tests:
```bash
python -m pytest functions/redirector/test_redirector.py
```

### Integration Testing

Test with real Firestore (dev environment):

```python
from google.cloud import firestore

def test_link_creation():
    db = firestore.Client(project='gb-qr-tracker-dev')
    # Create test link
    # Verify it exists
    # Clean up
```

### Manual Testing

**Test Redirector**:
```bash
# Create test link in Firestore
# Then test redirect
curl -v "https://go.rocket-letter.de/test-link-id"
```

**Test Upload Processor**:
```bash
# Upload test CSV
gsutil cp test.csv gs://gb-qr-tracker-dev.firebasestorage.app/uploads/dev/uid/campaignId/source/
# Check logs
gcloud functions logs read upload_processor --limit=50
```

**Test Health Monitor**:
```bash
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/health_monitor"
```

### Cloudflare Worker Testing

```bash
cd workers/redirector
npm test
```

## Code Patterns

### Error Handling

**Non-blocking errors** (don't block main flow):
```python
try:
    _db.collection('hits').add(hit)
except Exception:
    if LOG_HIT_ERRORS:
        logging.exception("Hit write failed")
    # Continue - don't block redirect
```

**Blocking errors** (fail fast):
```python
if not link_id or not ID_PATTERN.match(link_id):
    return ('Invalid link ID', 400)
```

### Batch Operations

**Firestore Batch Writes**:
```python
batch = db.batch()
batch.update(link_ref, {'hit_count': Increment(1)})
batch.set(business_ref, business_data, merge=True)
batch.commit()
```

**Batch Size Limit**: 500 operations per batch

### ID Generation

**Pattern**:
```python
def assign_final_ids(precomputed):
    # Group by base_id
    groups = defaultdict(list)
    for item in precomputed:
        groups[item['base_id']].append(item)
    
    # Query existing once per group
    for base_id, items in groups.items():
        taken = existing_variants_for_base(COL_LINKS, base_id)
        for item in items:
            item['final_id'] = next_id_from_cache(base_id, taken)
```

### Query Optimization

**Use Indexes**:
```python
# Good: Uses index on owner_id + last_hit_at
query = db.collection('links')\
    .where('owner_id', '==', uid)\
    .order_by('last_hit_at', direction=firestore.Query.DESCENDING)\
    .limit(10)
```

**Avoid**:
```python
# Bad: No index, scans all documents
query = db.collection('links')\
    .where('campaign_name', '==', name)\
    .order_by('last_hit_at')  # Missing composite index
```

### Logging

**Structured Logging**:
```python
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info("Processing upload", extra={
    "campaign_id": campaign_id,
    "file_size": file_size
})

logger.error("Upload failed", extra={
    "error": str(e),
    "campaign_id": campaign_id
}, exc_info=True)
```

### Type Hints

```python
from typing import Optional, Dict, List

def process_row(row: Dict[str, str]) -> Optional[str]:
    business_name: Optional[str] = row.get('business_name')
    if not business_name:
        return None
    return sanitize_id(business_name)
```

## Debugging

### View Function Logs

```bash
# Real-time logs
gcloud functions logs read redirector --follow

# Last 50 logs
gcloud functions logs read redirector --limit=50

# Filter by severity
gcloud functions logs read redirector --severity=ERROR
```

### Debug Locally

**Add Debugger**:
```python
import pdb; pdb.set_trace()  # Python debugger
```

**Print Debugging**:
```python
print(f"DEBUG: link_id={link_id}, destination={destination}")
```

### Firestore Debugging

**Query Documents**:
```bash
gcloud firestore documents get links/test-link-id
```

**Query Collection**:
```python
from google.cloud import firestore

db = firestore.Client()
links = db.collection('links').where('owner_id', '==', 'uid').stream()
for link in links:
    print(link.id, link.to_dict())
```

### Cloudflare Worker Debugging

**View Logs**:
```bash
cd workers/redirector
npx wrangler tail
```

**Test Locally**:
```bash
npm run dev
# Then test in browser or curl
```

### Common Issues

**HMAC Verification Fails**:
- Check secret matches in Worker and Function
- Verify timestamp is within 5-minute window
- Check for quotes in secret env var

**Link Not Found**:
- Verify link exists in Firestore
- Check link ID matches exactly
- Verify link is active

**Upload Processing Fails**:
- Check manifest.json syntax
- Verify ownerId exists
- Check Firestore permissions
- Review function logs

## Contributing

### Code Style

- Follow PEP 8 for Python
- Use type hints
- Document functions with docstrings
- Keep functions focused (single responsibility)

### Documentation

- Update README.md for user-facing changes
- Update API.md for API changes
- Update ARCHITECTURE.md for architectural changes
- Add code comments for complex logic

### Pull Request Process

1. Create feature branch
2. Make changes
3. Test locally and in dev
4. Update documentation
5. Create pull request
6. Address review comments
7. Merge after approval

### Migration Scripts

When changing data schema:

1. Create migration script in `scripts/`
2. Test on dev database first
3. Document schema changes
4. Provide rollback script if needed

**Example**:
```python
# scripts/migrate_new_field.py
def migrate_new_field():
    db = firestore.Client()
    links = db.collection('links').stream()
    for link in links:
        link.reference.update({'new_field': 'default_value'})
```

## Performance Tips

### Firestore

- Use batch writes (up to 500 ops)
- Use `select([])` for existence checks
- Create composite indexes for queries
- Use pagination for large result sets

### Functions

- Right-size memory allocation
- Use appropriate timeouts
- Minimize cold starts (keep functions warm)
- Use connection pooling where possible

### Cloudflare Worker

- Keep Worker code small (< 1MB)
- Minimize external API calls
- Cache static data when possible

## Security Best Practices

### Secrets

- Never commit secrets
- Use Secret Manager
- Rotate secrets regularly
- Use least-privilege service accounts

### Authentication

- Verify Firebase tokens
- Check custom claims (isAdmin)
- Validate ownership before operations

### Input Validation

- Validate all inputs
- Sanitize IDs (regex patterns)
- Check URL safety before redirects
- Limit file sizes for uploads

## Resources

- [Google Cloud Functions Docs](https://cloud.google.com/functions/docs)
- [Cloudflare Workers Docs](https://developers.cloudflare.com/workers/)
- [Firestore Docs](https://cloud.google.com/firestore/docs)
- [Functions Framework](https://github.com/GoogleCloudPlatform/functions-framework-python)

