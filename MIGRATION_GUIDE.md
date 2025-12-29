# Migration Guide

This guide explains how to apply and track database migrations for the GB QR Tracker project.

**Note**: For detailed step-by-step guides for specific migrations, see `docs/migrations/`.

## Prerequisites

Install required Python packages:
```bash
pip install pyyaml google-cloud-firestore
```

## Step-by-Step: Applying a Migration

### Example: Applying "Delete Test Hits" Migration

#### Step 1: Review the Migration

```bash
# Check migration info from YAML
python scripts/migration_tracker.py info 20241228_001
```

This shows:
- Migration name and description
- Script path
- Dependencies
- Whether it's reversible

#### Step 2: Run Migration in Dry-Run Mode (Dev)

```bash
# Always test in dev first with dry-run
python scripts/migrate_delete_test_hits.py \
  --dry-run \
  --project gb-qr-tracker-dev \
  --database "(default)"
```

Review the output to see what would be deleted.

#### Step 3: Run Migration in Dev

```bash
# Actually run the migration in dev
python scripts/migrate_delete_test_hits.py \
  --project gb-qr-tracker-dev \
  --database "(default)"
```

**Important**: Type 'DELETE' when prompted to confirm.

#### Step 4: Record Migration in Dev

```bash
# Record that the migration was applied in dev
python scripts/migration_tracker.py apply 20241228_001 \
  --env dev \
  --project gb-qr-tracker-dev \
  --by "your-email@example.com"
```

#### Step 5: Verify Migration Status

```bash
# Check that it was recorded
python scripts/migration_tracker.py status 20241228_001 --env dev --project gb-qr-tracker-dev

# List all migrations in dev
python scripts/migration_tracker.py list --env dev --project gb-qr-tracker-dev
```

#### Step 6: Test in Dev Environment

Verify that the migration worked correctly:
- Check that test hits were removed from `hits` collection
- Verify no production data was affected
- Test that health monitor still works

#### Step 7: Apply to Production (After Testing)

```bash
# Dry-run in prod first
python scripts/migrate_delete_test_hits.py \
  --dry-run \
  --project gb-qr-tracker \
  --database "(default)"

# Run in prod
python scripts/migrate_delete_test_hits.py \
  --project gb-qr-tracker \
  --database "(default)"

# Record in prod
python scripts/migration_tracker.py apply 20241228_001 \
  --env prod \
  --project gb-qr-tracker \
  --by "your-email@example.com"
```

#### Step 8: Update YAML Status (Optional)

Manually update `migrations.yaml` to mark status as `applied` if desired.

## Migration Commands Reference

### Check Migration Info
```bash
python scripts/migration_tracker.py info <migration_id>
```

### Record Migration
```bash
python scripts/migration_tracker.py apply <migration_id> \
  --env <dev|prod> \
  --by "your-email@example.com"
```

### Check Status
```bash
python scripts/migration_tracker.py status <migration_id> --env <dev|prod>
```

### List All Migrations
```bash
python scripts/migration_tracker.py list --env <dev|prod>
```

## Environment-Specific Projects

- **Dev**: `gb-qr-tracker-dev`
- **Prod**: `gb-qr-tracker`

The tracker automatically detects the project from `--env`, but you can override with `--project`.

## Best Practices

1. **Always dry-run first** - Preview changes before applying
2. **Test in dev first** - Apply to dev, verify, then apply to prod
3. **Record immediately** - Record the migration right after applying
4. **Check dependencies** - The tracker checks dependencies automatically
5. **Use descriptive --by** - Use your email or name for audit trail

## Troubleshooting

### Migration Already Recorded

If you see "Migration already recorded", you can:
- Check status: `python scripts/migration_tracker.py status <id> --env <env>`
- Overwrite if needed (will prompt for confirmation)

### Dependencies Not Satisfied

If dependencies aren't met:
- Check which dependencies are missing
- Apply dependencies first
- Or use `--skip-deps` to override (not recommended)

### YAML Not Found

If you see "migrations.yaml not found":
- Ensure you're running from the project root
- Check that `migrations.yaml` exists in the root directory

