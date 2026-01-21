# pip install google-cloud-firestore tqdm

from hashlib import sha1
from datetime import datetime, timezone

from google.cloud import firestore
from google.cloud.firestore import Client
from tqdm import tqdm

# =====================
# CONFIG (edit here)
# =====================
PROJECT_ID_DEV = "gb-qr-tracker-dev"
PROJECT_ID_PROD = "gb-qr-tracker"

# Read from default DB, write to "test" DB
SRC_DATABASE_ID = "(default)"   # read
DST_DATABASE_ID = "test"        # write

DRY_RUN = False  # no writes when True

# Print sampling
PRINT_LIMIT_LINKS     = 5
PRINT_LIMIT_HITS      = 5
PRINT_LIMIT_CREATIONS = 20

# Process only part of the data (fast tests)
FILTER_CAMPAIGN_CODE = None     # e.g. "groessig-01"; set to None to disable filtering
LINK_LIMIT = 100              # 0 => process all matched links
HIT_LIMIT  = 60                # 0 => process all matched hits

# Source (old) / Destination (new) collection names
LINKS_SRC      = "links"
HITS_SRC       = "hits"
BUSINESSES_SRC = "businesses"

LINKS_DST      = "links"
HITS_DST       = "hits"
CAMPAIGNS_DST  = "campaigns"
BUSINESSES_DST = "businesses"  # references will point here (in DST DB)

# Customer → owner_id mapping (fallback when owner_id not on SRC campaign)
CUSTOMER_OWNER_MAP = {
    "groessig": "9xVVm8dhRhNmInut2bYcUfyQeTV2",
    # add others as needed
}

# Prefer resolving hits from already-migrated DST links; fall back to SRC during DRY runs
PREFER_DST_LINK_LOOKUP = True

# =====================
# Firestore clients
# =====================
SRC_DB: Client = firestore.Client(project=PROJECT_ID_PROD, database=SRC_DATABASE_ID)
DST_DB: Client = firestore.Client(project=PROJECT_ID_DEV, database=DST_DATABASE_ID)

# =====================
# Counters
# =====================
COUNTERS = {
    "links_scanned": 0, "links_printed": 0, "links_suppressed": 0, "links_migrated": 0,
    "hits_scanned": 0,  "hits_printed": 0,  "hits_suppressed": 0,  "hits_migrated": 0,
    "campaigns_ensured_unique": 0, "campaigns_created": 0, "campaigns_creation_logs": 0,
    "targets_ensured_unique": 0,   "targets_created": 0,   "targets_creation_logs": 0,
}
_seen_campaign_ids = set()
_seen_target_ids = set()

# =====================
# Helpers
# =====================
def stable_id(*parts: str) -> str:
    h = sha1()
    for p in parts:
        h.update(p.encode("utf-8")); h.update(b"|")
    return h.hexdigest()[:20]

def dst_business_ref(business_id: str):
    return DST_DB.collection(BUSINESSES_DST).document(business_id)

def src_business_doc(business_id: str):
    return SRC_DB.collection(BUSINESSES_SRC).document(business_id)

def ensure_campaign(owner_id: str, code: str, now_ts):
    """Ensure campaign in DST_DB. Returns (campaign_ref, created_bool)."""
    doc_id = stable_id(owner_id, code)
    cref = DST_DB.collection(CAMPAIGNS_DST).document(doc_id)

    if doc_id not in _seen_campaign_ids:
        _seen_campaign_ids.add(doc_id)
        COUNTERS["campaigns_ensured_unique"] += 1

    snap = cref.get()
    created = False
    if not snap.exists and not DRY_RUN:
        cref.set({
            "code": code, "name": code, "owner_id": owner_id, "status": "draft",
            "created_at": now_ts, "updated_at": now_ts,
            "totals": {"hits": 0, "links": 0, "targets": 0, "unique_ips": 0}
        })
        created = True
        COUNTERS["campaigns_created"] += 1

    if COUNTERS["campaigns_creation_logs"] < PRINT_LIMIT_CREATIONS:
        print(("[OK] Created" if created else "[=] Ensured")
              + f" campaign '{code}' id={cref.id} owner={owner_id}")
        COUNTERS["campaigns_creation_logs"] += 1
    elif COUNTERS["campaigns_creation_logs"] == PRINT_LIMIT_CREATIONS:
        print("... further campaign ensure/create logs suppressed ...")
        COUNTERS["campaigns_creation_logs"] += 1

    return cref, created

def ensure_target(campaign_ref, business_id: str, now_ts):
    """Ensure target subdoc in DST_DB under the given campaign."""
    tid = stable_id(campaign_ref.id, business_id)
    tref = campaign_ref.collection("targets").document(tid)

    if tid not in _seen_target_ids:
        _seen_target_ids.add(tid)
        COUNTERS["targets_ensured_unique"] += 1

    snap = tref.get()
    created = False
    if not snap.exists and not DRY_RUN:
        tref.set({
            "business_id": business_id,
            "business_ref": dst_business_ref(business_id),
            "created_at": now_ts, "updated_at": now_ts,
        })
        created = True
        COUNTERS["targets_created"] += 1

    if COUNTERS["targets_creation_logs"] < PRINT_LIMIT_CREATIONS:
        print(("[OK] Created" if created else "[=] Ensured")
              + f" target id={tref.id} for campaign={campaign_ref.id} business={business_id}")
        COUNTERS["targets_creation_logs"] += 1
    elif COUNTERS["targets_creation_logs"] == PRINT_LIMIT_CREATIONS:
        print("... further target ensure/create logs suppressed ...")
        COUNTERS["targets_creation_logs"] += 1

    return tref, created

def coalesce_timestamp(ts):
    return ts if ts is not None else firestore.SERVER_TIMESTAMP

# =====================
# Migration: Links (SRC -> DST)
# =====================
def migrate_links():
    print("Scanning old links (SRC default DB) ...")
    q = SRC_DB.collection(LINKS_SRC)
    if FILTER_CAMPAIGN_CODE:
        q = q.where("campaign", "==", FILTER_CAMPAIGN_CODE)
    old_links = list(q.stream())
    if LINK_LIMIT and LINK_LIMIT > 0:
        old_links = old_links[:LINK_LIMIT]

    COUNTERS["links_scanned"] = len(old_links)
    print(f"Found {len(old_links)} links (after filters/limits)")

    now_ts = datetime.now(timezone.utc)
    batch = DST_DB.batch(); ops = 0
    by_campaign = {}
    printed = 0

    for s in tqdm(old_links, desc="Migrating links", unit="link"):
        d = s.to_dict() or {}
        campaign_code = d.get("campaign")                # may be None in SRC new schema
        customer_code = d.get("customer")                # fallback owner mapping
        campaign_ref_src = d.get("campaign_ref")         # SRC reference (don't write this to DST)
        business_id   = d.get("business_id") or (d.get("business") and d["business"].id)
        destination   = d.get("destination")
        template_id   = d.get("template")
        hit_count     = d.get("hit_count", 0)
        last_hit_at   = d.get("last_hit_at")

        owner_id = None

        # If campaign string missing, try reading from SRC campaign_ref
        if not campaign_code and campaign_ref_src:
            try:
                camp_snap = campaign_ref_src.get()
                if camp_snap.exists:
                    camp_data = camp_snap.to_dict() or {}
                    campaign_code = camp_data.get("code") or camp_data.get("name")
                    owner_id = camp_data.get("owner_id") or owner_id
            except Exception as e:
                print(f"[WARN] Could not read SRC campaign_ref for link {s.id}: {e}")

        # Resolve owner_id (prefer campaign.owner_id, fallback to map)
        owner_id = owner_id or CUSTOMER_OWNER_MAP.get(customer_code)

        if not campaign_code or not owner_id:
            print(f"[WARN] Skip link {s.id}: missing campaign_code/owner_id after SRC lookup")
            continue
        if not business_id:
            print(f"[WARN] Skip link {s.id}: missing business_id")
            continue

        # Ensure campaign/target in DST
        campaign_ref, _ = ensure_campaign(owner_id, campaign_code, now_ts)
        target_ref, _   = ensure_target(campaign_ref, business_id, now_ts)

        # Ref we write should point to DST businesses
        bref_dst = dst_business_ref(business_id)

        # Optional: read snapshot for mailing from SRC businesses
        snapshot = {}
        try:
            bdoc = src_business_doc(business_id).get()
            if bdoc.exists:
                bd = bdoc.to_dict() or {}
                street = (bd.get("street") or "").strip()
                hn     = (bd.get("house_number") or "").strip()
                addr   = " ".join(x for x in [street, hn] if x).strip()
                snapshot = {
                    "business_name": bd.get("business_name") or bd.get("name"),
                    "recipient_name": None,
                    "address_lines": [addr] if addr else [],
                    "city": bd.get("city"), "postcode": bd.get("postcode"), "country": "DE",
                }
        except Exception as e:
            print(f"[WARN] Snapshot lookup failed for business {business_id}: {e}")

        short_code = s.id
        new_link_ref = DST_DB.collection(LINKS_DST).document(short_code)
        data = {
            "active": d.get("active", True),
            "business_ref": bref_dst,
            "campaign_ref": campaign_ref,             # DST ref
            "owner_id": owner_id,
            "destination": destination,
            "template_id": template_id,
            "hit_count": hit_count,
            "last_hit_at": coalesce_timestamp(last_hit_at),
            "short_code": short_code,
            "created_at": d.get("created_at") or now_ts,
            "target_ref": target_ref,                 # DST ref
            "snapshot_mailing": snapshot or None,
            "updated_at": now_ts,
            "campaign_code": campaign_code,           # convenience for later lookups
        }

        if DRY_RUN:
            if printed < PRINT_LIMIT_LINKS:
                print(f"DRY RUN: would set link {short_code} (DST) -> {data}")
                COUNTERS["links_printed"] += 1; printed += 1
            elif printed == PRINT_LIMIT_LINKS:
                print("... further links suppressed ...")
                COUNTERS["links_suppressed"] += (len(old_links) - PRINT_LIMIT_LINKS)
                printed += 1
        else:
            batch.set(new_link_ref, data, merge=True)
            ops += 1; COUNTERS["links_migrated"] += 1

        # Totals
        key = campaign_ref.id
        agg = by_campaign.setdefault(key, {"links": 0, "targets": set(), "hits": 0, "iphashes": set()})
        agg["links"] += 1; agg["targets"].add(target_ref.id)

        if ops >= 400 and not DRY_RUN:
            batch.commit(); batch = DST_DB.batch(); ops = 0

    if ops and not DRY_RUN:
        batch.commit()

    return by_campaign

# =====================
# Migration: Hits (SRC -> DST; uses DST links if available)
# =====================
def migrate_hits(by_campaign):
    print("Scanning old hits (SRC default DB) ...")
    q = SRC_DB.collection(HITS_SRC)
    if FILTER_CAMPAIGN_CODE:
        q = q.where("campaign", "==", FILTER_CAMPAIGN_CODE)
    old_hits = list(q.stream())
    if HIT_LIMIT and HIT_LIMIT > 0:
        old_hits = old_hits[:HIT_LIMIT]

    COUNTERS["hits_scanned"] = len(old_hits)
    print(f"Found {len(old_hits)} hits (after filters/limits)")

    now_ts = datetime.now(timezone.utc)
    batch = DST_DB.batch(); ops = 0; printed = 0

    for s in tqdm(old_hits, desc="Migrating hits", unit="hit"):
        d = s.to_dict() or {}
        business_id    = d.get("business_id")
        campaign_code  = d.get("campaign")  # may be None in SRC new schema
        link_id        = d.get("link_id")
        template_id    = d.get("template")

        owner_id = None
        link_data = None
        campaign_code_from_link = None

        # Prefer link lookup from DST (if links were migrated), fall back to SRC
        if link_id:
            if PREFER_DST_LINK_LOOKUP:
                link_snap = DST_DB.collection(LINKS_DST).document(link_id).get()
                if link_snap.exists:
                    link_data = link_snap.to_dict() or {}
                    owner_id = link_data.get("owner_id")
                    # convenience field we added on links:
                    campaign_code_from_link = link_data.get("campaign_code") or campaign_code_from_link

            if not link_data:
                link_snap_src = SRC_DB.collection(LINKS_SRC).document(link_id).get()
                if link_snap_src.exists:
                    link_data = link_snap_src.to_dict() or {}
                    # Try to read SRC campaign_ref to get code/owner
                    camp_ref_src = link_data.get("campaign_ref")
                    if camp_ref_src:
                        try:
                            camp_snap = camp_ref_src.get()
                            if camp_snap.exists:
                                camp_data = camp_snap.to_dict() or {}
                                campaign_code_from_link = campaign_code_from_link or camp_data.get("code") or camp_data.get("name")
                                owner_id = owner_id or camp_data.get("owner_id")
                        except Exception as e:
                            print(f"[WARN] Could not read SRC campaign via link {link_id}: {e}")

        # Finalize owner_id
        if not owner_id:
            cust = (campaign_code or "").split("-")[0] if campaign_code else None
            owner_id = CUSTOMER_OWNER_MAP.get(cust)
        if not owner_id:
            print(f"[WARN] Skip hit {s.id}: cannot resolve owner_id")
            continue

        # Decide campaign_code to use
        campaign_code_effective = campaign_code or campaign_code_from_link
        if not campaign_code_effective:
            print(f"[WARN] Skip hit {s.id}: missing campaign code (hit+link)")
            continue

        # IMPORTANT: always ensure DST campaign_ref from code+owner
        campaign_ref, _ = ensure_campaign(owner_id, campaign_code_effective, now_ts)

        # Backfill business_id from link data if needed
        if not business_id and link_data:
            bref_from_link = link_data.get("business_ref")  # may be DST or SRC doc ref
            if bref_from_link:
                business_id = bref_from_link.id

        if not business_id:
            print(f"[WARN] Skip hit {s.id}: missing business_id (even after link lookup)")
            continue

        target_ref, _ = ensure_target(campaign_ref, business_id, now_ts)
        bref_dst = dst_business_ref(business_id)

        new_hit_ref = DST_DB.collection(HITS_DST).document(s.id)
        data = {
            "business_ref": bref_dst,
            "campaign_ref": campaign_ref,      # DST ref
            "target_ref": target_ref,          # DST ref
            "owner_id": owner_id,
            "template_id": template_id,
            "link_id": link_id,
            "ts": d.get("ts") or now_ts,
            "device_type": d.get("device_type"),
            "geo_city": d.get("geo_city"),
            "geo_country": d.get("geo_country"),
            "geo_lat": d.get("geo_lat"),
            "geo_lon": d.get("geo_lon"),
            "geo_region": d.get("geo_region"),
            "geo_source": d.get("geo_source"),
            "ip_hash": d.get("ip_hash"),
            "ua_browser": d.get("ua_browser"),
            "ua_os": d.get("ua_os"),
            "user_agent": d.get("user_agent"),
            "updated_at": now_ts,
        }

        if DRY_RUN:
            if printed < PRINT_LIMIT_HITS:
                print(f"DRY RUN: would set hit {s.id} (DST) -> {data}")
                COUNTERS["hits_printed"] += 1; printed += 1
            elif printed == PRINT_LIMIT_HITS:
                print("... further hits suppressed ...")
                COUNTERS["hits_suppressed"] += (len(old_hits) - PRINT_LIMIT_HITS)
                printed += 1
        else:
            batch.set(new_hit_ref, data, merge=True)
            ops += 1; COUNTERS["hits_migrated"] += 1

        # Totals
        key = campaign_ref.id
        agg = by_campaign.setdefault(key, {"links": 0, "targets": set(), "hits": 0, "iphashes": set()})
        agg["hits"] += 1
        if d.get("ip_hash"): agg["iphashes"].add(d["ip_hash"])
        agg["targets"].add(target_ref.id)

        if ops >= 400 and not DRY_RUN:
            batch.commit(); batch = DST_DB.batch(); ops = 0

    if ops and not DRY_RUN:
        batch.commit()

    return by_campaign

# =====================
# Totals + Summary (DST)
# =====================
def recompute_campaign_totals(by_campaign):
    print("Updating campaign totals (DST) ...")
    now_ts = datetime.now(timezone.utc)
    batch = DST_DB.batch(); ops = 0
    for campaign_id, agg in by_campaign.items():
        cref = DST_DB.collection(CAMPAIGNS_DST).document(campaign_id)
        totals = {
            "links": agg.get("links", 0),
            "targets": len(agg.get("targets", [])),
            "hits": agg.get("hits", 0),
            "unique_ips": len(agg.get("iphashes", [])),
        }
        if DRY_RUN:
            print(f"DRY RUN: would update campaign {campaign_id} totals -> {totals}")
        else:
            batch.set(cref, {"totals": totals, "updated_at": now_ts}, merge=True)
            ops += 1
        if ops >= 400 and not DRY_RUN:
            batch.commit(); batch = DST_DB.batch(); ops = 0
    if ops and not DRY_RUN:
        batch.commit()

def print_summary(by_campaign):
    print("\n================ MIGRATION SUMMARY ================")
    print(f"DRY_RUN: {DRY_RUN}")
    print("SRC DB: (default)   ->  DST DB: test\n")
    print("Documents scanned (from SRC):")
    print(f"  Links scanned: {COUNTERS['links_scanned']}")
    print(f"  Hits scanned:  {COUNTERS['hits_scanned']}")
    print("\nPrinted (sampled writes to DST):")
    print(f"  Links printed:     {COUNTERS['links_printed']}")
    print(f"  Links suppressed:  {COUNTERS['links_suppressed']}")
    print(f"  Hits printed:      {COUNTERS['hits_printed']}")
    print(f"  Hits suppressed:   {COUNTERS['hits_suppressed']}")
    print("\nCampaigns/Targets (in DST):")
    print(f"  Campaigns ensured (unique): {COUNTERS['campaigns_ensured_unique']}")
    print(f"  Campaigns created:          {COUNTERS['campaigns_created']}{' (would be created)' if DRY_RUN else ''}")
    print(f"  Targets ensured (unique):   {COUNTERS['targets_ensured_unique']}")
    print(f"  Targets created:            {COUNTERS['targets_created']}{' (would be created)' if DRY_RUN else ''}")
    if not DRY_RUN:
        print("\nWrites performed to DST:")
        print(f"  Links migrated: {COUNTERS['links_migrated']}")
        print(f"  Hits migrated:  {COUNTERS['hits_migrated']}")
    print("\nPer-campaign aggregates (computed during pass):")
    for cid, agg in by_campaign.items():
        print(f"  Campaign {cid}: links={agg.get('links',0)}, hits={agg.get('hits',0)}, "
              f"targets={len(agg.get('targets',[]))}, unique_ips={len(agg.get('iphashes',[]))}")
    print("===================================================\n")

# =====================
# Main
# =====================
def run():
    by_campaign = migrate_links()
    by_campaign = migrate_hits(by_campaign)
    recompute_campaign_totals(by_campaign)
    print_summary(by_campaign)

if __name__ == "__main__":
    run()
    print("Done.")
