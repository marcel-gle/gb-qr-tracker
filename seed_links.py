# seed_links.py
# Create links in Firestore WITHOUT overwriting existing docs.
# Uses DocumentReference.create() which fails if the doc already exists.
#
# CSV columns: id,destination,active,business_id,business_name,campaign,template
#
# Examples:
#   python seed_links.py links.csv
#   python seed_links.py --prefix INV --count 1000 --dest https://example.com/landing \
#       --business-id ACME --business-name "ACME GmbH" --campaign SPRING24 --template TPL-A \
#       --on-duplicate skip

import argparse
import csv
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists

db = firestore.Client()
links = db.collection('links')

def create_link(doc_id: str, destination: str, active: bool = True,
                business_id: str = None, business_name: str = None,
                campaign: str = None, template: str = None):
    payload = {
        'destination': destination,
        'active': bool(active),
        'hit_count': 0,
        'created_at': firestore.SERVER_TIMESTAMP,
        'last_hit_at': None,
    }
    if business_id:
        payload['business_id'] = str(business_id)
    if business_name:
        payload['business_name'] = business_name
    if campaign:
        payload['campaign'] = campaign
    if template:
        payload['template'] = template

    # create() will raise AlreadyExists if doc_id is taken
    links.document(doc_id).create(payload)

def seed_from_csv(path: str, on_duplicate: str):
    created, skipped, errors = 0, 0, 0
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc_id = row.get('id') or row.get('link_id')
            dest = row.get('destination') or row.get('url')
            active = str(row.get('active', 'true')).lower() != 'false'
            business_id = row.get('business_id') or row.get('company_id')
            business_name = row.get('business_name') or row.get('company')
            campaign = row.get('campaign')
            template = row.get('template')
            if not doc_id or not dest:
                print(f"[skip] Missing id/destination: {row}")
                skipped += 1
                continue
            try:
                create_link(doc_id, dest, active, business_id, business_name, campaign, template)
                print(f"[ok] {doc_id} -> {dest}")
                created += 1
            except AlreadyExists:
                if on_duplicate == 'error':
                    raise
                print(f"[skip-duplicate] {doc_id} already exists")
                skipped += 1
            except Exception as e:
                print(f"[error] {doc_id}: {e}")
                errors += 1
    print(f"Done. created={created} skipped={skipped} errors={errors}")

def seed_prefixed(prefix: str, count: int, dest: str, on_duplicate: str,
                  business_id: str = None, business_name: str = None,
                  campaign: str = None, template: str = None):
    created, skipped, errors = 0, 0, 0
    for i in range(count):
        doc_id = f"{prefix}{i+1}"
        try:
            create_link(doc_id, dest, True, business_id, business_name, campaign, template)
            if (i+1) % 100 == 0:
                print(f"[ok] Created {i+1} so far...")
            created += 1
        except AlreadyExists:
            if on_duplicate == 'error':
                raise
            print(f"[skip-duplicate] {doc_id} already exists")
            skipped += 1
        except Exception as e:
            print(f"[error] {doc_id}: {e}")
            errors += 1
    print(f"Done. created={created} skipped={skipped} errors={errors}")

if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Seed Firestore links without overwriting existing docs.')
    p.add_argument('csv', nargs='?', help='CSV: id,destination,active,business_id,business_name,campaign,template')
    p.add_argument('--prefix', help='Prefix for generated IDs, e.g. INV')
    p.add_argument('--count', type=int, default=0, help='How many IDs to generate')
    p.add_argument('--dest', help='Destination URL used for all generated IDs')
    p.add_argument('--business-id', help='Business identifier to attach to generated links')
    p.add_argument('--business-name', help='Business name to attach to generated links')
    p.add_argument('--campaign', help='Campaign label to attach to generated links')
    p.add_argument('--template', help='Template label to attach to generated links')
    p.add_argument('--on-duplicate', choices=['skip','error'], default='skip',
                   help='When a link already exists: skip (default) or raise error')
    args = p.parse_args()

    if args.csv:
        seed_from_csv(args.csv, args.on_duplicate)
    elif args.prefix and args.count > 0 and args.dest:
        seed_prefixed(args.prefix, args.count, args.dest, args.on_duplicate,
                      args.business_id, args.business_name, args.campaign, args.template)
    else:
        p.error('Provide either a CSV file or --prefix, --count, and --dest.')
