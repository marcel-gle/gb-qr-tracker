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


Example cli command:

python seed_links.py \
  --business-file "/path/to/businesses.xlsx" \
  --base-url "https://europe-west3-gb-qr-tracker.cloudfunctions.net/redirector" \
  --dest "https://groessig.de/weiterbildung-plus-lohnzuschuss" \
  --campaign "groessig-01" \
  --customer "groessig" \
  --limit 3 \
  --prefix "GROE-"
