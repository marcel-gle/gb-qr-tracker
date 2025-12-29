# Step-by-Step: Apply Migration 20241228_001

## Migration: Delete Test Hits from Hits Collection

This guide walks you through applying the migration to delete test hits from the `hits` collection.

---

## Prerequisites

Install PyYAML if not already installed:
```bash
pip install pyyaml
# or
pip3 install pyyaml
```

---

## Step 1: Review Migration Info

```bash
python3 scripts/migration_tracker.py info 20241228_001
```

**Expected output:**
- Migration name: "Delete Test Hits from Hits Collection"
- Script: `scripts/migrate_delete_test_hits.py`
- Description: Removes test hits from hits collection
- Status: pending

---

## Step 2: Dry-Run in Dev Environment

**Always test in dev first!**

```bash
python3 scripts/migrate_delete_test_hits.py \
  --dry-run \
  --project gb-qr-tracker-dev \
  --database "(default)"
```

**What to check:**
- How many test hits will be deleted
- Verify they're actually test hits (link_id starts with 'monitor-test', etc.)
- Make sure no production hits are included

**Expected output:**
```
[DRY-RUN] Would delete X test hits from hits collection
```

---

## Step 3: Apply Migration in Dev

**Only proceed if dry-run looks correct!**

```bash
python3 scripts/migrate_delete_test_hits.py \
  --project gb-qr-tracker-dev \
  --database "(default)"
```

**Important:**
- You'll be prompted to type 'DELETE' to confirm
- Review the output carefully
- Note how many hits were deleted

---

## Step 4: Record Migration in Dev

After successfully running the migration, record it:

```bash
python3 scripts/migration_tracker.py apply 20241228_001 \
  --env dev \
  --project gb-qr-tracker-dev \
  --by "your-email@example.com"
```

Replace `your-email@example.com` with your actual email.

**Expected output:**
```
✅ Recorded migration 20241228_001 as applied in dev
```

---

## Step 5: Verify Migration Status

Check that it was recorded correctly:

```bash
python3 scripts/migration_tracker.py status 20241228_001 \
  --env dev \
  --project gb-qr-tracker-dev
```

**Expected output:**
```
Migration: Delete Test Hits from Hits Collection
ID: 20241228_001
Environment: dev
Status: applied
Applied: [timestamp]
By: your-email@example.com
```

---

## Step 6: Verify in Dev Environment

Manually verify the migration worked:

1. **Check Firestore Console:**
   - Go to Firestore in GCP Console
   - Check `hits` collection - should have no test hits
   - Check `test_hits` collection - test hits should be there (from new health monitor)

2. **Test Health Monitor:**
   - Trigger health monitor manually
   - Verify it still works
   - Check that new test hits go to `test_hits` collection

3. **Verify Production Data:**
   - Check that production hits are still intact
   - Verify counters weren't affected

---

## Step 7: Apply to Production (After Dev Verification)

**Only proceed after verifying dev works correctly!**

### 7a: Dry-Run in Prod

```bash
python3 scripts/migrate_delete_test_hits.py \
  --dry-run \
  --project gb-qr-tracker \
  --database "(default)"
```

### 7b: Apply in Prod

```bash
python3 scripts/migrate_delete_test_hits.py \
  --project gb-qr-tracker \
  --database "(default)"
```

### 7c: Record in Prod

```bash
python3 scripts/migration_tracker.py apply 20241228_001 \
  --env prod \
  --project gb-qr-tracker \
  --by "your-email@example.com"
```

### 7d: Verify in Prod

```bash
python3 scripts/migration_tracker.py status 20241228_001 \
  --env prod \
  --project gb-qr-tracker
```

---

## Step 8: Update YAML (Optional)

If you want to manually track status in YAML, update `migrations.yaml`:

```yaml
- id: "20241228_001"
  status: "applied"
  applied_date: "2025-12-28"
  applied_by: "your-email@example.com"
  environments: ["dev", "prod"]
```

---

## Quick Reference

### Check Migration Info
```bash
python3 scripts/migration_tracker.py info 20241228_001
```

### Run Migration (Dry-Run)
```bash
# Dev
python3 scripts/migrate_delete_test_hits.py --dry-run --project gb-qr-tracker-dev

# Prod
python3 scripts/migrate_delete_test_hits.py --dry-run --project gb-qr-tracker
```

### Run Migration (Actual)
```bash
# Dev
python3 scripts/migrate_delete_test_hits.py --project gb-qr-tracker-dev

# Prod
python3 scripts/migrate_delete_test_hits.py --project gb-qr-tracker
```

### Record Migration
```bash
# Dev
python3 scripts/migration_tracker.py apply 20241228_001 --env dev --by "your-email@example.com"

# Prod
python3 scripts/migration_tracker.py apply 20241228_001 --env prod --by "your-email@example.com"
```

### Check Status
```bash
# Dev
python3 scripts/migration_tracker.py status 20241228_001 --env dev

# Prod
python3 scripts/migration_tracker.py status 20241228_001 --env prod
```

### List All Migrations
```bash
# Dev
python3 scripts/migration_tracker.py list --env dev

# Prod
python3 scripts/migration_tracker.py list --env prod
```

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'yaml'"
```bash
pip install pyyaml
```

### "Migration already recorded"
- Check status first: `python3 scripts/migration_tracker.py status 20241228_001 --env dev`
- If you need to overwrite, the script will prompt you

### "No test hits found"
- This is fine! It means the migration already happened or there were no test hits
- Check that health monitor is writing to `test_hits` collection

---

## Summary

✅ **Created:**
- `migrations.yaml` - Migration registry
- `scripts/migration_tracker.py` - Migration tracking tool
- `MIGRATION_GUIDE.md` - General migration guide
- `APPLY_MIGRATION_20241228_001.md` - This step-by-step guide

✅ **Next Steps:**
1. Install PyYAML: `pip install pyyaml`
2. Follow the steps above to apply the migration
3. Start with dev, verify, then apply to prod

