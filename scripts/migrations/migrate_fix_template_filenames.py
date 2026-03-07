#!/usr/bin/env python3
"""
Migration script to ensure Cloud Storage template PDF filenames match Firestore links.template_id.

For each links document, we treat links.template_id as canonical and make sure there is a
corresponding PDF under:

    uploads/{env}/{owner_id}/{campaign_id}/templates/{template_id}

where:
  - env is "dev" or "prod"
  - owner_id is links.owner_id
  - campaign_id is links.campaign_ref.id

If a template file exists under the campaign's templates/ folder but with a different filename,
this script can (when not in --dry-run mode) rename the blob so that its filename matches
links.template_id.

Usage examples:

    # Dry-run in dev, limit to first 500 links
    python scripts/migrations/migrate_fix_template_filenames.py \\
        --env dev \\
        --dry-run \\
        --limit-links 500

    # Real run in dev (all links)
    python scripts/migrations/migrate_fix_template_filenames.py --env dev

    # Real run in prod for a specific owner + campaign
    python scripts/migrations/migrate_fix_template_filenames.py \\
        --env prod \\
        --owner-id SOME_UID \\
        --campaign-id SOME_CAMPAIGN_ID
"""

import argparse
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import firestore
from google.cloud import storage

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm is optional
    def tqdm(x, **kwargs):
        return x


# -----------------------------
# Defaults (mirroring repo envs)
# -----------------------------

DEFAULT_DEV_PROJECT = "gb-qr-tracker-dev"
DEFAULT_PROD_PROJECT = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"

DEFAULT_DEV_BUCKET = "gb-qr-tracker-dev.firebasestorage.app"
DEFAULT_PROD_BUCKET = "gb-qr-tracker.firebasestorage.app"


@dataclass
class LinkRef:
    link_id: str
    owner_id: str
    campaign_id: str
    template_id: str


@dataclass
class GroupState:
    owner_id: str
    campaign_id: str
    templates_from_db: Set[str] = field(default_factory=set)
    links_per_template: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))


@dataclass
class RenamePlan:
    owner_id: str
    campaign_id: str
    env: str
    src_name: str
    dest_name: str
    template_id: str
    reason: str


def get_project_for_env(env: str) -> str:
    if env == "prod":
        return DEFAULT_PROD_PROJECT
    if env == "dev":
        return DEFAULT_DEV_PROJECT
    raise ValueError(f"Unknown env: {env!r}, expected 'dev' or 'prod'")


def get_bucket_for_env(env: str) -> str:
    if env == "prod":
        return DEFAULT_PROD_BUCKET
    if env == "dev":
        return DEFAULT_DEV_BUCKET
    raise ValueError(f"Unknown env: {env!r}, expected 'dev' or 'prod'")


# -----------------------------
# Firestore scanning
# -----------------------------

def scan_links_grouped(
    db: firestore.Client,
    limit_links: Optional[int] = None,
    owner_id_filter: Optional[str] = None,
    campaign_id_filter: Optional[str] = None,
) -> Tuple[int, Dict[Tuple[str, str], GroupState]]:
    """
    Scan the links collection and group by (owner_id, campaign_id).

    Returns:
        total_scanned: number of link documents scanned
        groups: mapping (owner_id, campaign_id) -> GroupState
    """
    links_ref = db.collection("links")

    page_size = 1000
    last_doc = None
    total_scanned = 0
    groups: Dict[Tuple[str, str], GroupState] = {}

    print("Scanning links collection to build (owner_id, campaign_id, template_id) map...")

    while True:
        query = links_ref.limit(page_size)
        if last_doc is not None:
            query = query.start_after(last_doc)

        batch = list(query.stream())
        if not batch:
            break

        for doc in tqdm(batch, desc="Scanning links", unit="link", leave=False):
            if limit_links is not None and total_scanned >= limit_links:
                print(f"Reached limit_links={limit_links}, stopping scan.")
                return total_scanned, groups

            total_scanned += 1

            try:
                data = doc.to_dict() or {}
            except Exception as e:
                print(f"[warn] Failed to read document {doc.id}: {e}", file=sys.stderr)
                continue

            owner_id = data.get("owner_id")
            template_id = data.get("template_id")
            campaign_ref = data.get("campaign_ref")

            if not owner_id or not template_id or not campaign_ref:
                # Missing required context; skip but log occasionally if needed
                continue

            campaign_id = getattr(campaign_ref, "id", None)
            if not campaign_id:
                continue

            if owner_id_filter and owner_id != owner_id_filter:
                continue
            if campaign_id_filter and campaign_id != campaign_id_filter:
                continue

            key = (str(owner_id), str(campaign_id))
            if key not in groups:
                groups[key] = GroupState(owner_id=str(owner_id), campaign_id=str(campaign_id))

            group = groups[key]
            group.templates_from_db.add(str(template_id))
            group.links_per_template[str(template_id)].append(doc.id)

        if len(batch) < page_size:
            break

        last_doc = batch[-1]

    return total_scanned, groups


# -----------------------------
# Storage helpers
# -----------------------------

def list_template_pdfs_for_group(
    bucket: storage.Bucket,
    env: str,
    owner_id: str,
    campaign_id: str,
) -> Tuple[Dict[str, storage.Blob], Set[str]]:
    """
    List template PDFs for a given (owner, campaign) group.

    Returns:
        blobs_by_name: filename -> Blob
        filenames: set of filenames
    """
    prefix = f"uploads/{env}/{owner_id}/{campaign_id}/templates"
    blobs_by_name: Dict[str, storage.Blob] = {}
    filenames: Set[str] = set()

    # Note: list_blobs(prefix=".../templates") returns both the 'templates' folder doc
    # and the files. We skip directory placeholders and non-PDFs.
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name
        if name.endswith("/"):
            continue
        filename = name.split("/")[-1]
        if not filename.lower().endswith(".pdf"):
            continue
        blobs_by_name[filename] = blob
        filenames.add(filename)

    return blobs_by_name, filenames


def _filename_match_score(template_id: str, candidate_filename: str) -> Optional[int]:
    """
    Simple heuristic scoring between canonical template_id and a candidate filename.

    Lower score = better match. None means 'no match'.
    """
    import os as _os

    t_base, t_ext = _os.path.splitext(template_id.lower())
    c_base, c_ext = _os.path.splitext(candidate_filename.lower())

    if c_ext != t_ext:
        # We only consider same extension as a strong signal; otherwise ignore.
        return None

    if template_id.lower() == candidate_filename.lower():
        return 0

    # Exact base match
    if t_base == c_base:
        return 1

    # Handle common '_qr_track' patterns (either side may have it)
    suffix = "_qr_track"
    if t_base.endswith(suffix) and c_base == t_base[: -len(suffix)]:
        return 2
    if c_base.endswith(suffix) and t_base == c_base[: -len(suffix)]:
        return 2

    # Base-name match ignoring case and certain punctuation variations
    simplified_t = "".join(ch for ch in t_base if ch.isalnum())
    simplified_c = "".join(ch for ch in c_base if ch.isalnum())
    if simplified_t and simplified_t == simplified_c:
        return 3

    return None


def plan_template_renames_for_group(
    env: str,
    owner_id: str,
    campaign_id: str,
    templates_from_db: Set[str],
    blobs_by_name: Dict[str, storage.Blob],
    storage_filenames: Set[str],
) -> Tuple[List[RenamePlan], List[Tuple[str, str]]]:
    """
    For a given (owner, campaign) group, compute which storage blobs should be renamed
    to match the Firestore templates_from_db.

    Returns:
        plans: list of RenamePlan objects
        unresolved: list of (template_id, reason) for which we couldn't safely plan a rename
    """
    plans: List[RenamePlan] = []
    unresolved: List[Tuple[str, str]] = []

    # Precompute for matching
    storage_list = list(storage_filenames)

    for template_id in sorted(templates_from_db):
        if template_id in storage_filenames:
            # Already perfect match
            continue

        # Attempt heuristic matching
        scored_candidates: List[Tuple[int, str]] = []
        for fname in storage_list:
            score = _filename_match_score(template_id, fname)
            if score is not None:
                scored_candidates.append((score, fname))

        if not scored_candidates:
            unresolved.append((template_id, "no candidate filename in storage"))
            continue

        # Choose the best (lowest score); if multiple with same score, treat as ambiguous
        scored_candidates.sort(key=lambda x: x[0])
        best_score, best_fname = scored_candidates[0]
        ambiguous = any(score == best_score and fname != best_fname for score, fname in scored_candidates[1:])
        if ambiguous:
            unresolved.append((template_id, f"ambiguous candidates in storage (best score {best_score})"))
            continue

        src_blob = blobs_by_name.get(best_fname)
        if not src_blob:
            unresolved.append((template_id, f"internal error: blob for {best_fname} not found"))
            continue

        dest_name = f"uploads/{env}/{owner_id}/{campaign_id}/templates/{template_id}"
        src_name = src_blob.name
        if src_name == dest_name:
            # Should not happen because template_id not in storage_filenames, but guard anyway
            continue

        plans.append(
            RenamePlan(
                owner_id=owner_id,
                campaign_id=campaign_id,
                env=env,
                src_name=src_name,
                dest_name=dest_name,
                template_id=template_id,
                reason=f"rename {best_fname} -> {template_id} (score={best_score})",
            )
        )

    return plans, unresolved


def apply_rename_plans(
    bucket: storage.Bucket,
    plans: List[RenamePlan],
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Apply or print rename plans (copy+delete within the same bucket).

    Returns:
        (applied_count, skipped_count)
    """
    applied = 0
    skipped = 0

    if not plans:
        return applied, skipped

    for plan in plans:
        if dry_run:
            print(
                f"[DRY-RUN] {plan.env} owner={plan.owner_id} campaign={plan.campaign_id} "
                f"template_id={plan.template_id}: {plan.src_name} -> {plan.dest_name} ({plan.reason})"
            )
            continue

        try:
            src_blob = bucket.blob(plan.src_name)
            if not src_blob.exists():
                print(f"[warn] Source blob missing, skipping: {plan.src_name}")
                skipped += 1
                continue

            dest_blob = bucket.blob(plan.dest_name)
            if dest_blob.exists():
                print(
                    f"[warn] Destination already exists, skipping rename: {plan.dest_name} "
                    f"(src={plan.src_name})"
                )
                skipped += 1
                continue

            # Copy + delete (emulate rename)
            new_blob = bucket.copy_blob(src_blob, bucket, new_name=plan.dest_name)
            # Preserve basic metadata where reasonable
            new_blob.cache_control = src_blob.cache_control
            new_blob.content_encoding = src_blob.content_encoding
            new_blob.content_language = src_blob.content_language
            new_blob.content_disposition = src_blob.content_disposition
            new_blob.content_type = src_blob.content_type
            new_blob.patch()

            src_blob.delete()

            print(
                f"[applied] {plan.env} owner={plan.owner_id} campaign={plan.campaign_id} "
                f"template_id={plan.template_id}: {plan.src_name} -> {plan.dest_name}"
            )
            applied += 1
        except Exception as e:
            print(
                f"[error] Failed to rename {plan.src_name} -> {plan.dest_name} "
                f"for template_id={plan.template_id}: {e}",
                file=sys.stderr,
            )
            skipped += 1

    return applied, skipped


# -----------------------------
# Main migration flow
# -----------------------------

def _interactive_resolve_unresolved(
    env: str,
    owner_id: str,
    campaign_id: str,
    unresolved: List[Tuple[str, str]],
    blobs_by_name: Dict[str, storage.Blob],
    storage_filenames: Set[str],
) -> Tuple[List[RenamePlan], List[Tuple[str, str]]]:
    """
    Allow the operator to interactively choose which existing storage file to
    rename for unresolved template_ids.

    For each unresolved template_id, we show the available storage filenames
    and let the user either:
      - select by index, or
      - type an exact filename present in storage, or
      - skip the template.
    """
    extra_plans: List[RenamePlan] = []
    still_unresolved: List[Tuple[str, str]] = []

    if not unresolved or not storage_filenames:
        return extra_plans, unresolved

    storage_sorted = sorted(storage_filenames)

    for template_id, reason in unresolved:
        print()
        print(f"  [interactive] Unresolved template_id={template_id}: {reason}")
        print("  Available storage files in this group:")
        for idx, fname in enumerate(storage_sorted, start=1):
            print(f"    [{idx}] {fname}")

        while True:
            choice = input(
                "  Choose file number to rename to this template_id "
                "(or enter exact filename, or 's' to skip): "
            ).strip()

            if not choice or choice.lower() == "s":
                print("  → Skipping this template_id (no manual mapping).")
                still_unresolved.append((template_id, reason))
                break

            # Try interpreting as index
            selected_fname: Optional[str] = None
            if choice.isdigit():
                idx = int(choice)
                if 1 <= idx <= len(storage_sorted):
                    selected_fname = storage_sorted[idx - 1]
                else:
                    print(f"  Invalid index {idx}. Please choose a number between 1 and {len(storage_sorted)}, or 's' to skip.")
                    continue
            else:
                # Treat as filename
                if choice in storage_filenames:
                    selected_fname = choice
                else:
                    print("  Filename not found in storage for this group. Please choose again or 's' to skip.")
                    continue

            if not selected_fname:
                print("  No filename selected; skipping this template_id.")
                still_unresolved.append((template_id, reason))
                break

            src_blob = blobs_by_name.get(selected_fname)
            if not src_blob:
                print(f"  Blob not found for chosen filename '{selected_fname}', skipping this template_id.")
                still_unresolved.append((template_id, reason))
                break

            dest_name = f"uploads/{env}/{owner_id}/{campaign_id}/templates/{template_id}"
            src_name = src_blob.name
            if src_name == dest_name:
                print("  Source and destination names are identical; nothing to rename. Skipping.")
                still_unresolved.append((template_id, reason))
                break

            plan = RenamePlan(
                owner_id=owner_id,
                campaign_id=campaign_id,
                env=env,
                src_name=src_name,
                dest_name=dest_name,
                template_id=template_id,
                reason=f"interactive choice {selected_fname} -> {template_id}",
            )
            extra_plans.append(plan)
            print(
                f"  → Planned interactive rename: {src_name} -> {dest_name} "
                f"(template_id={template_id})"
            )
            break

    return extra_plans, still_unresolved


def run_migration(
    env: str,
    project: Optional[str],
    database: str,
    bucket_name: Optional[str],
    owner_id_filter: Optional[str],
    campaign_id_filter: Optional[str],
    limit_links: Optional[int],
    dry_run: bool,
    interactive: bool = False,
    report_only: bool = False,
    exclude_groups: Optional[Set[Tuple[str, str]]] = None,
) -> None:
    project_id = project or get_project_for_env(env)
    storage_bucket_name = bucket_name or get_bucket_for_env(env)

    print("=== migrate_fix_template_filenames configuration ===")
    print(f"env:            {env}")
    print(f"project:        {project_id}")
    print(f"database:       {database}")
    print(f"bucket:         {storage_bucket_name}")
    print(f"owner filter:   {owner_id_filter or 'ALL'}")
    print(f"campaign filter:{campaign_id_filter or 'ALL'}")
    print(f"limit_links:    {limit_links if limit_links is not None else 'ALL'}")
    print(f"dry_run:        {dry_run}")
    print(f"interactive:    {interactive}")
    print(f"report_only:    {report_only}")
    if exclude_groups:
        pretty_excludes = ", ".join(f"{o}:{c}" for (o, c) in sorted(exclude_groups))
        print(f"exclude groups: {pretty_excludes}")
    else:
        print("exclude groups: NONE")
    print()

    db = firestore.Client(project=project_id, database=database)
    storage_client = storage.Client()
    bucket = storage_client.bucket(storage_bucket_name)

    total_scanned, groups = scan_links_grouped(
        db=db,
        limit_links=limit_links,
        owner_id_filter=owner_id_filter,
        campaign_id_filter=campaign_id_filter,
    )

    print()
    print(f"Scanned {total_scanned} links. Found {len(groups)} (owner_id, campaign_id) groups.")

    total_plans = 0
    total_unresolved = 0
    total_applied = 0
    total_skipped = 0

    for (owner_id, campaign_id), state in groups.items():
        if exclude_groups and (owner_id, campaign_id) in exclude_groups:
            print()
            print(f"--- Skipping excluded group owner={owner_id} campaign={campaign_id} ---")
            continue

        print()
        print(f"--- Processing group owner={owner_id} campaign={campaign_id} ---")
        blobs_by_name, storage_filenames = list_template_pdfs_for_group(
            bucket=bucket,
            env=env,
            owner_id=owner_id,
            campaign_id=campaign_id,
        )

        print(
            f"  DB templates: {len(state.templates_from_db)} | "
            f"storage PDFs: {len(storage_filenames)}"
        )

        # Detailed visibility into template names in Firestore vs Storage
        if state.templates_from_db:
            print("  Templates from links (template_id):")
            for tmpl in sorted(state.templates_from_db):
                print(f"    - {tmpl}")
        else:
            print("  Templates from links (template_id): NONE")

        if storage_filenames:
            print("  Template PDFs in storage (filenames):")
            for fname in sorted(storage_filenames):
                print(f"    - {fname}")
        else:
            print("  Template PDFs in storage (filenames): NONE")

        # In report-only mode we don't plan or apply any renames; we just show data.
        if report_only:
            print("  (report-only mode: no rename planning or renames applied for this group)")
            continue

        plans, unresolved = plan_template_renames_for_group(
            env=env,
            owner_id=owner_id,
            campaign_id=campaign_id,
            templates_from_db=state.templates_from_db,
            blobs_by_name=blobs_by_name,
            storage_filenames=storage_filenames,
        )

        # Optionally allow manual resolution of unresolved templates
        if interactive and unresolved and storage_filenames:
            manual_plans, still_unresolved = _interactive_resolve_unresolved(
                env=env,
                owner_id=owner_id,
                campaign_id=campaign_id,
                unresolved=unresolved,
                blobs_by_name=blobs_by_name,
                storage_filenames=storage_filenames,
            )
            if manual_plans:
                print(f"  Interactive plans added for this group: {len(manual_plans)}")
                plans.extend(manual_plans)
            unresolved = still_unresolved

        total_plans += len(plans)
        total_unresolved += len(unresolved)

        if unresolved:
            print("  Unresolved templates in this group:")
            for tmpl, reason in unresolved:
                print(f"    - template_id={tmpl}: {reason}")

        if plans:
            print(f"  Planned renames for this group: {len(plans)}")
            applied, skipped = apply_rename_plans(bucket=bucket, plans=plans, dry_run=dry_run)
            total_applied += applied
            total_skipped += skipped
        else:
            print("  No renames needed for this group.")

    print()
    print("=== Summary ===")
    print(f"Total links scanned:                      {total_scanned}")
    print(f"Total (owner_id, campaign_id) groups:     {len(groups)}")
    print(f"Total planned renames:                    {total_plans}")
    print(f"Total unresolved template_ids:            {total_unresolved}")
    if dry_run:
        print(f"Total renames that WOULD be applied:      {total_plans}")
    else:
        print(f"Total renames actually applied:           {total_applied}")
        print(f"Total renames skipped/failed:             {total_skipped}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Ensure Cloud Storage template PDF filenames under "
            "uploads/{env}/{uid}/{campaignId}/templates match Firestore links.template_id "
            "(using links.template_id as canonical)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run in dev for the first 500 links
  python scripts/migrations/migrate_fix_template_filenames.py --env dev --dry-run --limit-links 500

  # Real run in dev for all links
  python scripts/migrations/migrate_fix_template_filenames.py --env dev

  # Real run in prod for a specific owner and campaign
  python scripts/migrations/migrate_fix_template_filenames.py \\
      --env prod \\
      --owner-id SOME_UID \\
      --campaign-id SOME_CAMPAIGN_ID
        """,
    )

    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment (affects default project ID and storage bucket).",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="Override GCP project ID (otherwise derived from --env).",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore database ID (default: {DEFAULT_DATABASE_ID}).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="Override Cloud Storage bucket name (otherwise derived from --env).",
    )
    parser.add_argument(
        "--owner-id",
        type=str,
        default=None,
        help="Optional filter: only process links for this owner_id.",
    )
    parser.add_argument(
        "--campaign-id",
        type=str,
        default=None,
        help="Optional filter: only process links for this campaign_id.",
    )
    parser.add_argument(
        "--limit-links",
        type=int,
        default=None,
        help="Optional limit on the number of links to scan (for testing).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended operations without modifying Cloud Storage.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help=(
            "Enable interactive mode: for unresolved template_ids, prompt in the CLI "
            "to choose which existing storage file to rename."
        ),
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help=(
            "Only show, for each (owner_id, campaign_id), the unique template_ids from links "
            "and the template filenames present in storage; do not plan or apply any renames."
        ),
    )
    parser.add_argument(
        "--exclude-group",
        action="append",
        default=None,
        help=(
            "Exclude a specific (owner_id, campaign_id) group from processing. "
            "Format: OWNER_ID:CAMPAIGN_ID. Can be specified multiple times."
        ),
    )

    args = parser.parse_args()

    # Parse excluded groups of the form OWNER_ID:CAMPAIGN_ID
    exclude_groups: Set[Tuple[str, str]] = set()
    if args.exclude_group:
        for raw in args.exclude_group:
            try:
                owner, campaign = raw.split(":", 1)
                owner = owner.strip()
                campaign = campaign.strip()
                if owner and campaign:
                    exclude_groups.add((owner, campaign))
                else:
                    print(f"[warn] Ignoring invalid --exclude-group value (empty owner or campaign): {raw}", file=sys.stderr)
            except ValueError:
                print(f"[warn] Ignoring invalid --exclude-group value (expected OWNER_ID:CAMPAIGN_ID): {raw}", file=sys.stderr)

    try:
        run_migration(
            env=args.env,
            project=args.project,
            database=args.database,
            bucket_name=args.bucket,
            owner_id_filter=args.owner_id,
            campaign_id_filter=args.campaign_id,
            limit_links=args.limit_links,
            dry_run=args.dry_run,
            interactive=args.interactive,
            report_only=args.report_only,
            exclude_groups=exclude_groups or None,
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[fatal] Migration failed: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

