# Testing the docs_to_letter Function

## Document Access Requirements

**The document does NOT need to be public**, but the service account running the Cloud Function must have access to it.

### How to Grant Access

1. **Get the service account email** from your deployment:

   - Check `config.dev.sh` or the service account used in `deploy.sh`
   - Format: `cf-campaign-importer@gb-qr-tracker-dev.iam.gserviceaccount.com` (or similar)

2. **Share the Google Doc with the service account**:

   - Open your Google Doc
   - Click "Share" button
   - Add the service account email as a viewer or editor
   - The service account email will look like: `xxxxx@xxxxx.iam.gserviceaccount.com`

3. **Alternative: Use a test document**:

   - Create a test Google Doc
   - Share it with the service account email
   - Use this document ID for testing

### Why This Works

The function uses Application Default Credentials (ADC) which authenticates as the service account attached to the Cloud Function. Google Docs API requires explicit sharing permissions - the service account acts like a user who needs to be granted access.

## Testing the Deployed Function

### 1. Get the Function URL

After deployment, the function URL will be:

```
https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/migrate_document
```

You can also get it via:

```bash
gcloud functions describe migrate_document \
  --project=gb-qr-tracker-dev \
  --region=europe-west3 \
  --gen2 \
  --format="value(serviceConfig.uri)"
```

### 2. Test with curl

Since the function is deployed with `--allow-unauthenticated`, you can call it directly:

```bash
# Replace DOCUMENT_ID with your actual Google Doc ID
# The document ID is in the URL: https://docs.google.com/document/d/DOCUMENT_ID/edit

curl -X POST \
  https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/migrate_document \
  -H "Content-Type: application/json" \
  -d '{
    "document_id": "YOUR_GOOGLE_DOC_ID"
  }'
```

### 3. Expected Response

**Success response:**

```json
{
  "success": true,
  "template_id": "abc123",
  "template_url": "https://app.templated.io/editor/abc123",
  "pages_migrated": 1
}
```

**Error response:**

```json
{
  "error": "Error message here"
}
```

### 4. Common Issues and Solutions

**Issue: "Error fetching Google Doc"**

- Verify the service account has access to the document
- Check the document ID is correct (extract from Google Docs URL)
- Ensure the document exists and is not deleted

**Issue: "TEMPLATED_IO_KEY environment variable is not set"**

- Verify the environment variable is set in the deployment
- Check `config.dev.sh` includes `TEMPLATED_IO_KEY=$TEMPLATED_IO_KEY`
- Ensure `.env.dev` has `TEMPLATED_IO_KEY` defined

**Issue: "Failed to create template"**

- Check the Templated.io API key is valid
- Verify the API endpoint is correct
- Check function logs for detailed error messages

### 5. View Function Logs

```bash
# View recent logs
gcloud functions logs read migrate_document \
  --project=gb-qr-tracker-dev \
  --region=europe-west3 \
  --gen2 \
  --limit=50

# Or view in Console:
# https://console.cloud.google.com/functions/details/europe-west3/migrate_document?project=gb-qr-tracker-dev&tab=logs
```

### 6. Extract Document ID from Google Docs URL

The document ID is the long string between `/d/` and `/edit` in the URL:

```
https://docs.google.com/document/d/1a2b3c4d5e6f7g8h9i0j/edit
                                    ^^^^^^^^^^^^^^^^
                                    This is the document_id
```

## Summary

- **Document access**: Share the Google Doc with the service account email (not public)
- **Testing**: POST JSON with `document_id` to the function URL
- **No authentication needed**: Function is deployed with `--allow-unauthenticated`
- **Check logs**: Use `gcloud functions logs read` or Cloud Console for debugging

