## Migration 20260206_001 – Fix Template Filenames to Match `links.template_id`

This guide explains how to run migration **20260206_001** which ensures that
Cloud Storage template PDF filenames under
`uploads/{env}/{uid}/{campaignId}/templates` match the canonical
`links.template_id` field in Firestore.

The migration:
- Scans the `links` collection in Firestore.
- Groups links by `(owner_id, campaign_id)` and collects `template_id` values.
- Lists existing template PDFs in the corresponding Storage folder.
- Plans safe renames (copy+delete) so filenames match `links.template_id`,
  using simple heuristics to map old filenames to canonical ones.

### Prerequisites

- Python environment with the project dependencies installed (including
  `google-cloud-firestore`, `google-cloud-storage`, and optionally `tqdm`).
- Credentials that allow you to read from Firestore and read/write to the
  Firebase Storage bucket for the chosen environment.

### Script Location

- Script: `scripts/migrations/migrate_fix_template_filenames.py`
- Migration ID: `20260206_001`

### 1. Dry-run in Dev

Run a dry-run in **dev** first. This prints the planned renames without
modifying Cloud Storage:

```bash
python scripts/migrations/migrate_fix_template_filenames.py \
  --env dev \
  --dry-run \
  --limit-links 500
```

Review the output, especially:
- The number of planned renames.
- Any unresolved `template_id` values in each `(owner_id, campaign_id)` group.

You can remove `--limit-links` to scan all links once you’re confident:

```bash
python scripts/migrations/migrate_fix_template_filenames.py \
  --env dev \
  --dry-run
```

Optionally, you can target a specific owner or campaign:

```bash
python scripts/migrations/migrate_fix_template_filenames.py \
  --env dev \
  --dry-run \
  --owner-id YOUR_UID \
  --campaign-id YOUR_CAMPAIGN_ID
```

### 2. Real Run in Dev

When the dry-run output looks correct, run the migration for real in **dev**:

```bash
python scripts/migrations/migrate_fix_template_filenames.py \
  --env dev
```

This will perform copy+delete operations inside the dev bucket to rename
template PDFs so that their filenames match `links.template_id`.

### 3. Record Migration in Dev

After a successful run in dev, record the migration using the migration
tracker:

```bash
python scripts/migrations/migration_tracker.py apply 20260206_001 \
  --env dev \
  --project gb-qr-tracker-dev \
  --by "your-email@example.com"
```

You can verify the status:

```bash
python scripts/migrations/migration_tracker.py status 20260206_001 \
  --env dev \
  --project gb-qr-tracker-dev
```

### 4. Dry-run in Prod

After validating dev, repeat the dry-run in **prod**:

```bash
python scripts/migrations/migrate_fix_template_filenames.py \
  --env prod \
  --dry-run
```

Again, review the planned operations and unresolved `template_id` values.

### 5. Real Run in Prod

When you’re satisfied with the dry-run, run the migration for real in **prod**:

```bash
python scripts/migrations/migrate_fix_template_filenames.py \
  --env prod
```

### 6. Record Migration in Prod

Finally, record the migration as applied in **prod**:

```bash
python scripts/migrations/migration_tracker.py apply 20260206_001 \
  --env prod \
  --project gb-qr-tracker \
  --by "your-email@example.com"
```

You can verify:

```bash
python scripts/migrations/migration_tracker.py status 20260206_001 \
  --env prod \
  --project gb-qr-tracker
```

