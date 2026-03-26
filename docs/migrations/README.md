# Migration Documentation

This folder contains detailed step-by-step guides for specific migrations.

## Structure

- `APPLY_MIGRATION_<ID>.md` - Step-by-step guides for applying specific migrations
- Each guide includes:
  - Prerequisites
  - Step-by-step instructions for dev and prod
  - Verification steps
  - Troubleshooting

## General Migration Guide

For general migration procedures and commands, see [MIGRATION_GUIDE.md](../../MIGRATION_GUIDE.md) in the project root.

## Migration Registry

All migrations are registered in [migrations.yaml](../../migrations.yaml) in the project root.

## Backfill Overlay Campaign IDs

Use `scripts/migrations/backfill_campaign_ids.py` to backfill `campaign_ids` on:

- `customers/{customerId}/businesses/{businessId}`

This migration derives campaign membership from `links` (source of truth), using:

- `owner_id` -> `customerId`
- `business_ref.id` -> normalized `businessId` (same sanitize/normalize semantics as backend upload flow)
- `campaign_ref.id` -> campaign membership value

### Prerequisites

- Python environment with project dependencies installed.
- Firestore access via ADC (Application Default Credentials), for example:
  - `gcloud auth application-default login`, or
  - `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json`
- Correct project/database selection (use `--project` and `--database` as needed).

### Dry Run (no writes)

```bash
python scripts/migrations/backfill_campaign_ids.py --dry-run --project gb-qr-tracker-dev --database test
```

### Scoped Test Example

```bash
python scripts/migrations/backfill_campaign_ids.py \
  --dry-run \
  --campaign-id <campaignId> \
  --customer-id <customerId> \
  --limit 500
```

### Full Run Example

```bash
python scripts/migrations/backfill_campaign_ids.py \
  --batch-size 450 \
  --workers 4
```

### Verification Example (read-only)

```bash
python scripts/migrations/backfill_campaign_ids.py \
  --verify \
  --project gb-qr-tracker-dev \
  --database test
```

### Optional Behavior

- Skip creating missing overlays:

```bash
python scripts/migrations/backfill_campaign_ids.py --skip-missing-overlays
```

### Rollback Approach

This is a deterministic backfill from `links` as source of truth.
If results need correction, fix source `links` and re-run the migration (or run verification and re-run scoped subsets).

