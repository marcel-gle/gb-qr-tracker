# Trackable Links MVP


## Campaigns, Templates & Detailed Analytics

- Add `campaign` and `template` to each link (via CSV or CLI flags).
- Each scan writes a document to `hits` with server timestamp (`ts`) and parsed user-agent fields:
  `device_type`, `ua_browser`, `ua_os`, plus `referer` if present.
- Optional data retention via TTL: set env `HIT_TTL_DAYS=30` (or any number), and enable TTL on `hits.expires_at` in the Firestore console.

**Deploy with TTL example:**
```bash
gcloud functions deploy redirector   --gen2 --runtime=python311 --region=europe-west3   --entry-point=redirector --source=.   --trigger-http --allow-unauthenticated   --set-env-vars=HIT_TTL_DAYS=30
```

**Seed with campaign/template:**
```bash
python seed_links.py --prefix INV --count 100   --dest https://example.com/landing   --business-id ACME --business-name "ACME GmbH"   --campaign SPRING25 --template HERO-A
```

**Privacy note:** This code does **not** store IPs. User-agent/referrer may still be considered personal data depending on context—use TTL and keep retention minimal if you need raw per-hit logs.

Update google cloud functions:

Check active project:
- gcloud config get-value project


DEV:
gcloud functions deploy redirector --gen2 --runtime=python311 --region=europe-west3 \
  --entry-point=redirector --source=. --trigger-http --allow-unauthenticated \
  --set-env-vars='HIT_TTL_DAYS=30,GEOIP_API_URL=https://ipapi.co/{ip}/json/,STORE_IP_HASH=1,IP_HASH_SALT=4hinbwhifi3adc42cr2r2334c43cc2ipt8k8,LOG_HIT_ERRORS=1'


PROD:



Example cli command:

python seed_links.py \
  --business-file "/path/to/businesses.xlsx" \
  --base-url "https://europe-west3-gb-qr-tracker.cloudfunctions.net/redirector" \
  --dest "https://groessig.de/weiterbildung-plus-lohnzuschuss" \
  --campaign "groessig-01" \
  --customer "groessig" \
  --limit 3 \
  --prefix "GROE-"


NEW Example CLI command to deploy redirector:
 gcloud functions deploy redirector \
  --gen2 \
  --runtime=python311 \
  --region=europe-west3 \
  --entry-point=redirector \
  --source=functions/redirector \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars=HIT_TTL_DAYS=30,GEOIP_API_URL=https://ipapi.co/{ip}/json/,STORE_IP_HASH=1,IP_HASH_SALT=4hinbwhifi3adc42cr2r2334c43cc2ipt8k8,LOG_HIT_ERRORS=1



deploy upload

gcloud functions deploy upload_processor \
  --project=$PROJECT_ID \
  --gen2 \
  --region=europe-west3 \
  --runtime=python311 \
  --source=functions/upload_processor \
  --entry-point=process_business_upload \
  --service-account=$SA \
  --trigger-bucket=gb-qr-tracker-dev.firebasestorage.app \
  --trigger-location=eu \
  --trigger-service-account=$SA \
  --set-secrets=MAPBOX_TOKEN=projects/$PROJECT_ID/secrets/MAPBOX_TOKEN:latest \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,DATABASE_ID="(default)" \
  --memory=1GiB \
  --timeout=540s


