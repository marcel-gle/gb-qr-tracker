#!/usr/bin/env python3
"""
Migrate Callcenter V1 conversation docs to Callcenter V2 root calls docs.

Source:
  customers/{customerId}/businesses/{businessId}/conversations/{conversationId}

Destination:
  calls/{callId}

Key features:
- Idempotent writes using deterministic destination IDs and legacy_source_path marker.
- Legacy outcome -> structured call_process mapping.
- Overlay backfill to customers/{customerId}/businesses/{businessId}/latest_call_process.
- Optional cleanup of latest_outcome.
- Verification summaries (counts + sample timeline checks).
- Optional --exclude-customer-id to skip customers (e.g. very large business lists, no calls).
- Resume: default checkpoint file (multi-customer runs) records completed customers; --reset-checkpoint to start over.
- Streams use google.api_core retry to reduce transient gRPC / query-timeout failures.

Usage examples:
  # Dry-run on dev
  python scripts/migrations/migrate_callcenter_v2_calls.py --dry-run

  # Test on a single customer (paginated businesses, default batch 50 + progress bar)
  python scripts/migrations/migrate_callcenter_v2_calls.py --dry-run --customer-id YOUR_UID
  python scripts/migrations/migrate_callcenter_v2_calls.py --customer-id YOUR_UID --business-batch-size 100
  python scripts/migrations/migrate_callcenter_v2_calls.py --customer-id YOUR_UID --business-batch-size 0

  # Execute on prod default DB
  python scripts/migrations/migrate_callcenter_v2_calls.py --env prod

  # Execute on explicit project/database and remove latest_outcome
  python scripts/migrations/migrate_callcenter_v2_calls.py \
    --project gb-qr-tracker \
    --database "(default)" \
    --cleanup-latest-outcome

  # Full migration but skip a huge customer with no conversations (repeat flag for multiple)
  python scripts/migrations/migrate_callcenter_v2_calls.py --exclude-customer-id HEAVY_UID

  # Resume after a crash (default checkpoint path; omit --no-checkpoint)
  python scripts/migrations/migrate_callcenter_v2_calls.py --env dev --reset-checkpoint   # first run optional
  python scripts/migrations/migrate_callcenter_v2_calls.py --env dev                    # continues from checkpoint
"""

import argparse
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from google.cloud import firestore
from google.cloud.firestore_v1 import DELETE_FIELD, SERVER_TIMESTAMP
from tqdm import tqdm

try:
    from google.api_core import exceptions as core_exceptions
    from google.api_core import retry as api_retry

    STREAM_RETRY = api_retry.Retry(
        initial=0.5,
        maximum=120.0,
        multiplier=1.5,
        predicate=api_retry.if_exception_type(
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
            core_exceptions.InternalServerError,
            core_exceptions.ResourceExhausted,
            core_exceptions.Aborted,
        ),
        timeout=600.0,
    )
except Exception:  # pragma: no cover - minimal envs
    core_exceptions = None  # type: ignore
    STREAM_RETRY = None

try:
    from google.cloud.firestore_v1.field_path import FieldPath
except Exception:  # pragma: no cover - older client builds
    FieldPath = None


DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"
BATCH_SIZE = 400
CALLS_COLLECTION = "calls"
DEFAULT_BUSINESS_BATCH_SINGLE_CUSTOMER = 50
CHECKPOINT_VERSION = 1


def _stream_retry_kw() -> Dict[str, Any]:
    if STREAM_RETRY is None:
        return {}
    return {"retry": STREAM_RETRY}


def _retryable_firestore_error(exc: BaseException) -> bool:
    if core_exceptions is not None and isinstance(
        exc,
        (
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
            core_exceptions.InternalServerError,
            core_exceptions.ResourceExhausted,
            core_exceptions.Aborted,
        ),
    ):
        return True
    text = str(exc).lower()
    return any(
        s in text
        for s in (
            "unavailable",
            "deadline exceeded",
            "deadline",
            "timeout",
            "connection reset",
            "connection refused",
            "try again",
            "503",
            "504",
        )
    )


def list_stream_resilient(stream_factory: Callable[[], Any], label: str, attempts: int = 6) -> list:
    """
    Materialize a Firestore stream with retries (full re-read on failure).
    """
    last: Optional[BaseException] = None
    for attempt in range(attempts):
        try:
            return list(stream_factory())
        except Exception as e:
            last = e
            if attempt == attempts - 1 or not _retryable_firestore_error(e):
                raise
            delay = min(90.0, 2.0**attempt)
            print(
                f"[WARN] {label} transient error ({type(e).__name__}: {e}); "
                f"retry {attempt + 2}/{attempts} in {delay:.1f}s"
            )
            time.sleep(delay)
    raise last  # pragma: no cover


def default_checkpoint_path(project_id: str, database_id: str) -> str:
    safe = database_id.replace("(", "").replace(")", "").replace(" ", "_") or "default"
    return f".migrate_callcenter_v2_{project_id}_{safe}.json"


def load_checkpoint(path: str, project_id: str, database_id: str) -> Set[str]:
    if not os.path.isfile(path):
        return set()
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if data.get("project_id") != project_id or data.get("database") != database_id:
        raise ValueError(
            f"Checkpoint {path!r} targets project={data.get('project_id')!r} "
            f"database={data.get('database')!r}; this run is "
            f"project={project_id!r} database={database_id!r}. "
            "Use --reset-checkpoint or delete the file."
        )
    return set(data.get("completed_customer_ids", []))


def save_checkpoint(path: str, completed: Set[str], project_id: str, database_id: str) -> None:
    tmp = f"{path}.{os.getpid()}.tmp"
    payload = {
        "version": CHECKPOINT_VERSION,
        "project_id": project_id,
        "database": database_id,
        "completed_customer_ids": sorted(completed),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def _document_id_order_field():
    return FieldPath.document_id() if FieldPath else "__name__"


def iter_business_documents(
    businesses_ref,
    batch_size: Optional[int],
    pbar: Optional[Any] = None,
):
    """
    Yield business documents under customers/.../businesses.
    If batch_size is set, page with order_by(document_id) + limit (smaller reads per round-trip).
    If pbar is set, increment per business; when paging, postfix shows batch index.
    """
    fp = _document_id_order_field()
    skw = _stream_retry_kw()
    if batch_size and batch_size > 0:
        query = businesses_ref.order_by(fp).limit(batch_size)
        batch_num = 0
        while True:

            def stream_this_page():
                return query.stream(**skw)

            docs = list_stream_resilient(
                stream_this_page,
                label=f"businesses page (batch {batch_num + 1})",
            )
            batch_num += 1
            if not docs:
                break
            if pbar is not None:
                pbar.set_postfix_str(f"batch {batch_num} (+{len(docs)})", refresh=False)
            for d in docs:
                if pbar is not None:
                    pbar.update(1)
                yield d
            if len(docs) < batch_size:
                break
            query = businesses_ref.order_by(fp).limit(batch_size).start_after(docs[-1])
    else:
        for d in list_stream_resilient(
            lambda: businesses_ref.stream(**skw),
            label="businesses stream",
        ):
            if pbar is not None:
                pbar.update(1)
            yield d


def effective_business_batch_size(
    customer_id: Optional[str],
    business_batch_size_arg: Optional[int],
) -> Optional[int]:
    """
    Resolve page size for listing businesses.
    - Explicit > 0: use that page size.
    - 0: disable paging (single stream).
    - None with --customer-id: default DEFAULT_BUSINESS_BATCH_SINGLE_CUSTOMER.
    - None without --customer-id: no paging (legacy behaviour).
    """
    if business_batch_size_arg == 0:
        return None
    if business_batch_size_arg is not None and business_batch_size_arg > 0:
        return business_batch_size_arg
    if customer_id:
        return DEFAULT_BUSINESS_BATCH_SINGLE_CUSTOMER
    return None


def get_project_for_env(env: str) -> str:
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def normalize_business_id(business_id: str) -> str:
    return (business_id or "").strip()


def make_deterministic_call_id(customer_id: str, business_id: str, conversation_id: str) -> str:
    raw = f"{customer_id}|{business_id}|{conversation_id}"
    return "legacy_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def legacy_outcome_to_call_process(outcome: str) -> Optional[Dict[str, Optional[str]]]:
    """
    Maps legacy conversation `outcome` strings (Callcenter V1 UI / types) to V2 call_process.

    Legacy values:
      interested      — Interessiert
      callback        — Rückruf vereinbart
      not_interested  — Nicht interessiert
      not_reached     — Nicht erreicht
      no_answer       — type-only; stored on some conversations; same reach bucket as not_reached
      wrong_number    — type-only; same reach bucket as not_reached
    """
    if not outcome:
        return None

    oc = str(outcome).strip().lower()
    table = {
        # Nicht erreicht / could not meaningfully connect
        "not_reached": {"reach": "not_reached", "gatekeeper": None, "interest": None, "appointment": None},
        "no_answer": {"reach": "not_reached", "gatekeeper": None, "interest": None, "appointment": None},
        "wrong_number": {"reach": "not_reached", "gatekeeper": None, "interest": None, "appointment": None},
        # Reached; outcome of the conversation
        "interested": {"reach": "reached", "gatekeeper": "passed", "interest": "interested", "appointment": None},
        "callback": {"reach": "reached", "gatekeeper": "passed", "interest": "callback", "appointment": None},
        "not_interested": {"reach": "reached", "gatekeeper": "passed", "interest": "not_interested", "appointment": None},
    }
    return table.get(oc)


def parse_existing_call_process(data: Dict) -> Optional[Dict[str, Optional[str]]]:
    cp = data.get("call_process")
    if isinstance(cp, dict):
        return {
            "reach": cp.get("reach"),
            "gatekeeper": cp.get("gatekeeper"),
            "interest": cp.get("interest"),
            "appointment": cp.get("appointment"),
        }
    return None


def pick_latest_ts(data: Dict):
    return data.get("call_time") or data.get("created_at") or data.get("updated_at")


def build_v2_call_payload(
    db: firestore.Client,
    customer_id: str,
    business_id: str,
    conversation_id: str,
    source_data: Dict,
) -> Dict:
    now = datetime.now(timezone.utc)
    business_ref = (
        db.collection("customers")
        .document(customer_id)
        .collection("businesses")
        .document(business_id)
    )

    call_process = parse_existing_call_process(source_data)
    if call_process is None and source_data.get("outcome"):
        call_process = legacy_outcome_to_call_process(source_data.get("outcome"))

    created_at = source_data.get("created_at") or now
    updated_at = source_data.get("updated_at") or now
    call_time = source_data.get("call_time") or created_at

    status = source_data.get("status") or "successful"
    call_note = source_data.get("call_note")
    if call_note is None:
        call_note = source_data.get("note")

    owner_id = source_data.get("owner_id")
    created_by_uid = source_data.get("created_by_uid") or source_data.get("createdByUid") or owner_id

    legacy_source_path = (
        f"customers/{customer_id}/businesses/{business_id}/conversations/{conversation_id}"
    )

    payload = {
        "customer_id": customer_id,
        "business_id": normalize_business_id(business_id),
        "business_ref": business_ref,
        "owner_id": owner_id,
        "status": status,
        "call_process": call_process,
        "call_note": call_note if call_note is not None else None,
        "created_at": created_at,
        "updated_at": updated_at,
        "call_time": call_time,
        "created_by_uid": created_by_uid,
        "legacy_source_path": legacy_source_path,
        "migrated_at": SERVER_TIMESTAMP,
        "schema_version": "callcenter_v2_migrated",
    }
    return payload


def _normalize_exclude_customer_ids(ids: Optional[List[str]]) -> Set[str]:
    if not ids:
        return set()
    return {s.strip() for s in ids if s and str(s).strip()}


def scan_customer_conversation_rows(
    customer_doc: Any,
    single_customer_mode: bool,
    business_batch_size: Optional[int],
) -> List[Tuple[str, str, str, Dict]]:
    """
    Read all legacy conversation rows for one customer document.
    """
    cid = customer_doc.id
    rows: List[Tuple[str, str, str, Dict]] = []
    show_business_pbar = single_customer_mode or (business_batch_size is not None)
    businesses_ref = customer_doc.reference.collection("businesses")
    pbar_biz: Optional[Any] = None
    if show_business_pbar:
        pbar_biz = tqdm(
            unit="biz",
            desc=f"Businesses · {cid}",
            leave=single_customer_mode,
        )
    try:
        for business_doc in iter_business_documents(businesses_ref, business_batch_size, pbar_biz):
            business_id = business_doc.id

            def stream_conversations(bd=business_doc):
                return bd.reference.collection("conversations").stream(**_stream_retry_kw())

            conv_docs = list_stream_resilient(
                stream_conversations,
                label=f"conversations · {cid}/{business_id}",
            )
            for conv_doc in conv_docs:
                rows.append((cid, business_id, conv_doc.id, conv_doc.to_dict() or {}))
    finally:
        if pbar_biz is not None:
            pbar_biz.close()
    return rows


def new_aggregate_migration_stats() -> Dict[str, Any]:
    return {
        "legacy_seen": 0,
        "calls_created_or_updated": 0,
        "calls_skipped_idempotent": 0,
        "calls_missing_call_process": 0,
        "errors": [],
        "per_customer_counts": defaultdict(lambda: {"old": 0, "new": 0}),
    }


def merge_migration_stats(agg: Dict[str, Any], part: Dict[str, Any]) -> None:
    agg["legacy_seen"] += part["legacy_seen"]
    agg["calls_created_or_updated"] += part["calls_created_or_updated"]
    agg["calls_skipped_idempotent"] += part["calls_skipped_idempotent"]
    agg["calls_missing_call_process"] += part["calls_missing_call_process"]
    agg["errors"].extend(part["errors"])
    for cid, c in part["per_customer_counts"].items():
        agg["per_customer_counts"][cid]["old"] += c["old"]
        agg["per_customer_counts"][cid]["new"] += c["new"]


def merge_latest_overlays(
    acc: Dict[Tuple[str, str], Tuple[Any, Optional[Dict[str, Optional[str]]]]],
    part: Dict[Tuple[str, str], Tuple[Any, Optional[Dict[str, Optional[str]]]]],
) -> None:
    for key, (ts_new, cp_new) in part.items():
        cur = acc.get(key)
        if cur is None:
            acc[key] = (ts_new, cp_new)
            continue
        ts_old, _ = cur
        if ts_new is not None and (ts_old is None or ts_new > ts_old):
            acc[key] = (ts_new, cp_new)


def filter_overlays_for_customer(
    latest: Dict[Tuple[str, str], Tuple[Any, Optional[Dict[str, Optional[str]]]]],
    customer_id: str,
) -> Dict[Tuple[str, str], Tuple[Any, Optional[Dict[str, Optional[str]]]]]:
    return {k: v for k, v in latest.items() if k[0] == customer_id}


def new_overlay_aggregate_stats() -> Dict[str, Any]:
    return {"overlay_updates": 0, "overlay_skipped_no_process": 0, "errors": []}


def merge_overlay_aggregate_stats(agg: Dict[str, Any], part: Dict[str, Any]) -> None:
    agg["overlay_updates"] += part["overlay_updates"]
    agg["overlay_skipped_no_process"] += part["overlay_skipped_no_process"]
    agg["errors"].extend(part["errors"])


def load_customer_documents_for_migration(
    db: firestore.Client,
    customer_limit: Optional[int],
    customer_id: Optional[str],
    excluded: Set[str],
) -> Tuple[List[Any], str, bool]:
    """
    Return (customer DocumentSnapshots to process, tqdm desc label, single_customer_mode).
    """
    if customer_id is not None:
        cid = customer_id.strip()
        if not cid:
            print("[WARN] --customer-id is empty; nothing to migrate.")
            return [], "Customer", True
        if cid in excluded:
            print(f"[WARN] --customer-id {cid!r} is excluded; nothing to migrate.")
            return [], f"Customer {cid}", True
        snap = db.collection("customers").document(cid).get()
        if not snap.exists:
            print(f"[WARN] customers/{cid} does not exist; nothing to migrate.")
            return [], f"Customer {cid}", True
        return [snap], f"Customer {cid}", True

    customers_query = db.collection("customers")
    if customer_limit:
        customers_query = customers_query.limit(customer_limit)

    def stream_customers():
        return customers_query.stream(**_stream_retry_kw())

    customer_docs = list_stream_resilient(stream_customers, label="customers collection")
    return customer_docs, "Customers", False


def migrate_calls(
    db: firestore.Client,
    rows: List[Tuple[str, str, str, Dict]],
    dry_run: bool,
) -> Dict:
    stats = {
        "legacy_seen": len(rows),
        "calls_created_or_updated": 0,
        "calls_skipped_idempotent": 0,
        "calls_missing_call_process": 0,
        "errors": [],
    }
    latest_per_overlay: Dict[Tuple[str, str], Tuple[object, Optional[Dict[str, Optional[str]]]]] = {}
    per_customer_counts = defaultdict(lambda: {"old": 0, "new": 0})
    batch = db.batch()
    ops = 0

    for customer_id, business_id, conversation_id, source_data in tqdm(rows, desc="Migrating calls"):
        per_customer_counts[customer_id]["old"] += 1
        call_id = make_deterministic_call_id(customer_id, business_id, conversation_id)
        dest_ref = db.collection(CALLS_COLLECTION).document(call_id)

        try:
            payload = build_v2_call_payload(db, customer_id, business_id, conversation_id, source_data)
            cp = payload.get("call_process")
            if cp is None:
                stats["calls_missing_call_process"] += 1

            existing = dest_ref.get()
            if existing.exists:
                existing_data = existing.to_dict() or {}
                if existing_data.get("legacy_source_path") == payload.get("legacy_source_path"):
                    stats["calls_skipped_idempotent"] += 1
                else:
                    msg = (
                        f"ID collision on {dest_ref.path}: existing legacy_source_path="
                        f"{existing_data.get('legacy_source_path')!r}"
                    )
                    stats["errors"].append(msg)
                    continue
            else:
                if not dry_run:
                    batch.set(dest_ref, payload, merge=True)
                    ops += 1
                stats["calls_created_or_updated"] += 1
                per_customer_counts[customer_id]["new"] += 1

            overlay_key = (customer_id, business_id)
            ts = payload.get("call_time")
            current = latest_per_overlay.get(overlay_key)
            if current is None or (ts and current[0] and ts > current[0]):
                latest_per_overlay[overlay_key] = (ts, cp)

            if ops >= BATCH_SIZE and not dry_run:
                batch.commit()
                batch = db.batch()
                ops = 0
        except Exception as e:
            stats["errors"].append(f"{customer_id}/{business_id}/{conversation_id}: {e}")

    if ops > 0 and not dry_run:
        batch.commit()

    stats["latest_per_overlay"] = latest_per_overlay
    stats["per_customer_counts"] = dict(per_customer_counts)
    return stats


def backfill_overlays(
    db: firestore.Client,
    latest_per_overlay: Dict[Tuple[str, str], Tuple[object, Optional[Dict[str, Optional[str]]]]],
    dry_run: bool,
    cleanup_latest_outcome: bool,
) -> Dict:
    stats = {"overlay_updates": 0, "overlay_skipped_no_process": 0, "errors": []}
    batch = db.batch()
    ops = 0

    for (customer_id, business_id), (_, call_process) in tqdm(latest_per_overlay.items(), desc="Backfilling overlays"):
        overlay_ref = (
            db.collection("customers")
            .document(customer_id)
            .collection("businesses")
            .document(business_id)
        )
        if not call_process:
            stats["overlay_skipped_no_process"] += 1
            continue

        patch = {
            "latest_call_process": call_process,
            "updated_at": SERVER_TIMESTAMP,
        }
        if cleanup_latest_outcome:
            patch["latest_outcome"] = DELETE_FIELD

        try:
            if not dry_run:
                batch.set(overlay_ref, patch, merge=True)
                ops += 1
            stats["overlay_updates"] += 1
            if ops >= BATCH_SIZE and not dry_run:
                batch.commit()
                batch = db.batch()
                ops = 0
        except Exception as e:
            stats["errors"].append(f"{overlay_ref.path}: {e}")

    if ops > 0 and not dry_run:
        batch.commit()

    return stats


def run_timeline_spot_checks(
    db: firestore.Client,
    latest_per_overlay: Dict[Tuple[str, str], Tuple[object, Optional[Dict[str, Optional[str]]]]],
    max_checks: int,
) -> List[str]:
    checks = []
    sample_keys = list(latest_per_overlay.keys())[:max_checks]
    for customer_id, business_id in sample_keys:
        q = (
            db.collection(CALLS_COLLECTION)
            .where("customer_id", "==", customer_id)
            .where("business_id", "==", business_id)
            .order_by("call_time", direction=firestore.Query.DESCENDING)
            .limit(1)
        )
        docs = list_stream_resilient(
            lambda: q.stream(**_stream_retry_kw()),
            label=f"timeline · {customer_id}/{business_id}",
        )
        if not docs:
            checks.append(f"{customer_id}/{business_id}: no migrated call found")
            continue
        d = docs[0].to_dict() or {}
        checks.append(
            f"{customer_id}/{business_id}: latest_call_time={d.get('call_time')} "
            f"latest_call_process={d.get('call_process')}"
        )
    return checks


def print_summary(
    project_id: str,
    database: str,
    dry_run: bool,
    migration_stats: Dict,
    overlay_stats: Dict,
    timeline_checks: List[str],
):
    print("\n" + "=" * 72)
    print("Callcenter V2 Migration Summary")
    print("=" * 72)
    print(f"Project: {project_id}")
    print(f"Database: {database}")
    print(f"Dry run: {dry_run}")
    print("-" * 72)
    print(f"Legacy conversations scanned:   {migration_stats['legacy_seen']}")
    print(f"Calls created/updated:          {migration_stats['calls_created_or_updated']}")
    print(f"Calls skipped (idempotent):     {migration_stats['calls_skipped_idempotent']}")
    print(f"Calls with missing call_process:{migration_stats['calls_missing_call_process']}")
    print(f"Overlays updated:               {overlay_stats['overlay_updates']}")
    print(f"Overlays skipped no process:    {overlay_stats['overlay_skipped_no_process']}")
    print("-" * 72)

    per_customer = migration_stats.get("per_customer_counts", {})
    if per_customer:
        print("Per-customer parity (old vs newly created this run):")
        for customer_id, c in per_customer.items():
            print(f"  {customer_id}: old={c['old']} new={c['new']}")

    if timeline_checks:
        print("-" * 72)
        print("Timeline spot checks:")
        for line in timeline_checks:
            print(f"  {line}")

    all_errors = migration_stats.get("errors", []) + overlay_stats.get("errors", [])
    if all_errors:
        print("-" * 72)
        print(f"Errors ({len(all_errors)}):")
        for err in all_errors[:25]:
            print(f"  - {err}")
        if len(all_errors) > 25:
            print(f"  ... and {len(all_errors) - 25} more")
    print("=" * 72)


def run_migration(
    env: str,
    project: Optional[str],
    database: str,
    dry_run: bool,
    customer_limit: Optional[int],
    customer_id: Optional[str],
    business_batch_size_arg: Optional[int],
    exclude_customer_ids: Optional[List[str]],
    checkpoint_file: Optional[str],
    no_checkpoint: bool,
    reset_checkpoint: bool,
    timeline_checks: int,
    cleanup_latest_outcome: bool,
):
    project_id = project or get_project_for_env(env)
    db = firestore.Client(project=project_id, database=database)

    if customer_id and customer_limit is not None:
        print("[WARN] --customer-id is set; ignoring --customer-limit for this run.")

    excluded = _normalize_exclude_customer_ids(exclude_customer_ids)
    if customer_id and customer_id.strip() in excluded:
        print(
            f"[FATAL] --customer-id {customer_id.strip()!r} is also listed in "
            "--exclude-customer-id; remove one of the flags.",
            file=sys.stderr,
        )
        sys.exit(1)

    biz_batch = effective_business_batch_size(customer_id, business_batch_size_arg)
    biz_batch_display = str(biz_batch) if biz_batch else "off (single stream)"
    exclude_display = ", ".join(sorted(excluded)) if excluded else "(none)"

    is_single_customer = bool(customer_id and customer_id.strip())
    default_cp = default_checkpoint_path(project_id, database)
    active_checkpoint = (
        checkpoint_file.strip()
        if checkpoint_file and str(checkpoint_file).strip()
        else default_cp
    )
    use_checkpoint = not is_single_customer and not dry_run and not no_checkpoint

    if reset_checkpoint and os.path.isfile(active_checkpoint):
        os.remove(active_checkpoint)
        print(f"[CHECKPOINT] Reset removed {active_checkpoint!r}")

    completed: Set[str] = set()
    if use_checkpoint:
        try:
            completed = load_checkpoint(active_checkpoint, project_id, database)
        except ValueError as e:
            print(f"[FATAL] {e}", file=sys.stderr)
            sys.exit(1)
        if completed:
            print(
                f"[CHECKPOINT] Resuming {len(completed)} completed customer(s); "
                f"file={active_checkpoint!r}"
            )

    cp_banner = (
        f"on -> {active_checkpoint!r}"
        if use_checkpoint
        else (
            "off (single-customer)"
            if is_single_customer
            else ("off (dry-run)" if dry_run else "off (--no-checkpoint)")
        )
    )

    print("=" * 72)
    print("Callcenter V1 -> V2 migration")
    print("=" * 72)
    print(f"Project:                {project_id}")
    print(f"Database:               {database}")
    print(f"Dry run:                {dry_run}")
    print(f"Single customer only:   {customer_id or '(all)'}")
    print(f"Customer scan limit:    {customer_limit if not customer_id else 'n/a'}")
    print(f"Excluded customers:     {exclude_display}")
    print(f"Customer checkpoint:    {cp_banner}")
    print(f"Business batch size:    {biz_batch_display}")
    print(f"Timeline checks:        {timeline_checks}")
    print(f"Cleanup latest_outcome: {cleanup_latest_outcome}")
    print()

    customer_docs, desc_pbar, single_mode = load_customer_documents_for_migration(
        db=db,
        customer_limit=customer_limit if not customer_id else None,
        customer_id=customer_id,
        excluded=excluded,
    )

    agg_m = new_aggregate_migration_stats()
    global_latest: Dict[Tuple[str, str], Tuple[Any, Optional[Dict[str, Optional[str]]]]] = {}
    agg_o = new_overlay_aggregate_stats()

    if customer_docs:
        for customer_doc in tqdm(customer_docs, desc=desc_pbar):
            cid = customer_doc.id
            if cid in excluded:
                print(f"[SKIP] Excluding customer {cid} (--exclude-customer-id)")
                continue
            if use_checkpoint and cid in completed:
                print(f"[CHECKPOINT] Skip already completed customer {cid}")
                continue

            rows = scan_customer_conversation_rows(
                customer_doc,
                single_customer_mode=single_mode,
                business_batch_size=biz_batch,
            )
            part_m = migrate_calls(db=db, rows=rows, dry_run=dry_run)
            merge_migration_stats(agg_m, part_m)
            merge_latest_overlays(global_latest, part_m["latest_per_overlay"])

            sub_o = filter_overlays_for_customer(part_m["latest_per_overlay"], cid)
            part_o = backfill_overlays(
                db=db,
                latest_per_overlay=sub_o,
                dry_run=dry_run,
                cleanup_latest_outcome=cleanup_latest_outcome,
            )
            merge_overlay_aggregate_stats(agg_o, part_o)

            if use_checkpoint:
                completed.add(cid)
                save_checkpoint(active_checkpoint, completed, project_id, database)
                print(
                    f"[CHECKPOINT] Saved {len(completed)} customer(s) complete -> {active_checkpoint!r}"
                )
    else:
        print("[WARN] No customer documents to process.")

    agg_m["latest_per_overlay"] = global_latest
    agg_m["per_customer_counts"] = dict(agg_m["per_customer_counts"])

    timeline: List[str] = []
    if timeline_checks > 0 and global_latest:
        timeline = run_timeline_spot_checks(
            db=db, latest_per_overlay=global_latest, max_checks=timeline_checks
        )

    print_summary(
        project_id=project_id,
        database=database,
        dry_run=dry_run,
        migration_stats=agg_m,
        overlay_stats=agg_o,
        timeline_checks=timeline,
    )

    if dry_run:
        print("\n[DRY-RUN] No writes were committed.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate Callcenter conversations to V2 calls collection.")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--project", type=str, default=None, help="GCP project id (overrides --env)")
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE_ID)
    parser.add_argument("--dry-run", action="store_true", help="Preview operations without writes")
    parser.add_argument(
        "--customer-id",
        type=str,
        default=None,
        metavar="UID",
        help="Migrate only this customer (Firestore customers/{UID}); skips scanning all customers",
    )
    parser.add_argument("--customer-limit", type=int, default=None, help="Limit number of customers scanned")
    parser.add_argument(
        "--exclude-customer-id",
        action="append",
        default=None,
        metavar="UID",
        dest="exclude_customer_ids",
        help=(
            "Skip this customers/{UID} entirely (no businesses/conversations scan). "
            "Use multiple times to exclude several UIDs (e.g. huge business lists with no calls)."
        ),
    )
    parser.add_argument(
        "--business-batch-size",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Page size when listing customers/.../businesses (order_by document ID). "
            f"With --customer-id, defaults to {DEFAULT_BUSINESS_BATCH_SINGLE_CUSTOMER} if omitted. "
            "Use 0 to disable paging (one stream per customer). "
            "Without --customer-id, default is off unless you set this."
        ),
    )
    parser.add_argument("--timeline-checks", type=int, default=10, help="Number of business timeline spot checks")
    parser.add_argument(
        "--checkpoint-file",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "JSON file listing completed customer UIDs (multi-customer runs only). "
            "Default: .migrate_callcenter_v2_<project>_<database>.json in cwd if checkpoint is enabled."
        ),
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Do not read/write customer checkpoint (full scan every run; multi-customer only).",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Delete checkpoint file before run (same path as --checkpoint-file or default).",
    )
    parser.add_argument(
        "--cleanup-latest-outcome",
        action="store_true",
        help="Delete overlay latest_outcome field after writing latest_call_process",
    )
    args = parser.parse_args()

    try:
        run_migration(
            env=args.env,
            project=args.project,
            database=args.database,
            dry_run=args.dry_run,
            customer_limit=args.customer_limit,
            customer_id=args.customer_id,
            business_batch_size_arg=args.business_batch_size,
            exclude_customer_ids=args.exclude_customer_ids,
            checkpoint_file=args.checkpoint_file,
            no_checkpoint=args.no_checkpoint,
            reset_checkpoint=args.reset_checkpoint,
            timeline_checks=args.timeline_checks,
            cleanup_latest_outcome=args.cleanup_latest_outcome,
        )
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
