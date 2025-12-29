#!/usr/bin/env python3
"""
Migration tracking utilities for Firestore migrations.

Tracks which migrations have been applied in dev and prod environments.

Usage:
    # Record a migration as applied
    python migration_tracker.py apply 20241228_001 --env dev --project gb-qr-tracker-dev --by "admin@example.com"
    
    # Check migration status
    python migration_tracker.py status 20241228_001 --env dev --project gb-qr-tracker-dev
    
    # List all migrations for an environment
    python migration_tracker.py list --env dev --project gb-qr-tracker-dev
    
    # Show migration info from YAML
    python migration_tracker.py info 20241228_001
"""

import os
import sys
import argparse
import yaml
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from pathlib import Path
from google.cloud import firestore

# Default configuration
DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
MIGRATIONS_COLLECTION = "_migrations"
MIGRATIONS_YAML = "migrations.yaml"


def load_migrations_yaml() -> Dict[str, Any]:
    """Load migrations from YAML file."""
    yaml_path = Path(__file__).parent.parent / MIGRATIONS_YAML
    if not yaml_path.exists():
        print(f"Warning: {MIGRATIONS_YAML} not found", file=sys.stderr)
        return {"migrations": []}
    
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f) or {"migrations": []}


def get_migration_info(migration_id: str) -> Optional[Dict[str, Any]]:
    """Get migration info from YAML."""
    data = load_migrations_yaml()
    for migration in data.get("migrations", []):
        if migration.get("id") == migration_id:
            return migration
    return None


def get_project_for_env(env: str) -> str:
    """Get project ID for environment."""
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    elif env == "dev":
        return DEFAULT_PROJECT_DEV
    else:
        raise ValueError(f"Unknown environment: {env}. Use 'dev' or 'prod'")


def record_migration(
    db: firestore.Client,
    migration_id: str,
    migration_name: str,
    script_path: str,
    description: str,
    applied_by: str,
    environment: str,
    status: str = "applied"
) -> None:
    """Record a migration in the _migrations collection."""
    # Document ID includes environment to track per-environment
    doc_id = f"{migration_id}_{environment}"
    migration_ref = db.collection(MIGRATIONS_COLLECTION).document(doc_id)
    
    # Check if already exists
    existing = migration_ref.get()
    if existing.exists:
        existing_data = existing.to_dict()
        if existing_data.get("status") == "applied":
            print(f"⚠️  Migration {migration_id} already recorded as applied in {environment}")
            response = input("Overwrite? (yes/no): ")
            if response.lower() != "yes":
                print("Cancelled.")
                return
    
    migration_data = {
        "migration_id": migration_id,
        "migration_name": migration_name,
        "script_path": script_path,
        "description": description,
        "status": status,
        "environment": environment,
        "applied_at": firestore.SERVER_TIMESTAMP,
        "applied_by": applied_by,
        "recorded_at": firestore.SERVER_TIMESTAMP,
    }
    
    migration_ref.set(migration_data, merge=True)
    print(f"✅ Recorded migration {migration_id} as {status} in {environment}")


def get_migration_status(
    db: firestore.Client,
    migration_id: str,
    environment: str
) -> Optional[Dict[str, Any]]:
    """Get the status of a migration in a specific environment."""
    doc_id = f"{migration_id}_{environment}"
    migration_ref = db.collection(MIGRATIONS_COLLECTION).document(doc_id)
    migration_doc = migration_ref.get()
    
    if not migration_doc.exists:
        return None
    
    return migration_doc.to_dict()


def list_migrations(db: firestore.Client, environment: str) -> List[Dict[str, Any]]:
    """List all recorded migrations for an environment."""
    migrations_ref = db.collection(MIGRATIONS_COLLECTION)
    query = migrations_ref.where("environment", "==", environment)
    migrations = list(query.order_by("applied_at", direction=firestore.Query.DESCENDING).stream())
    
    return [m.to_dict() for m in migrations]


def check_dependencies(migration_id: str, environment: str, project: str) -> bool:
    """Check if migration dependencies are satisfied."""
    migration_info = get_migration_info(migration_id)
    if not migration_info:
        print(f"Warning: Migration {migration_id} not found in {MIGRATIONS_YAML}")
        return True  # Continue if not in YAML
    
    dependencies = migration_info.get("dependencies", [])
    if not dependencies:
        return True
    
    db = firestore.Client(project=project)
    
    print(f"Checking dependencies for {migration_id}...")
    all_satisfied = True
    for dep_id in dependencies:
        status = get_migration_status(db, dep_id, environment)
        if not status or status.get("status") != "applied":
            print(f"  ❌ Dependency {dep_id} not applied in {environment}")
            all_satisfied = False
        else:
            print(f"  ✅ Dependency {dep_id} is applied")
    
    return all_satisfied


def main():
    parser = argparse.ArgumentParser(
        description="Track Firestore migrations",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Apply command
    apply_parser = subparsers.add_parser("apply", help="Record a migration as applied")
    apply_parser.add_argument("migration_id", help="Migration ID (e.g., 20241228_001)")
    apply_parser.add_argument("--env", required=True, choices=["dev", "prod"], help="Environment (dev or prod)")
    apply_parser.add_argument("--project", help="GCP Project ID (auto-detected from env if not provided)")
    apply_parser.add_argument("--database", default="(default)", help="Firestore Database ID")
    apply_parser.add_argument("--by", default="unknown", help="Who applied the migration")
    apply_parser.add_argument("--skip-deps", action="store_true", help="Skip dependency checking")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check migration status")
    status_parser.add_argument("migration_id", help="Migration ID")
    status_parser.add_argument("--env", required=True, choices=["dev", "prod"], help="Environment")
    status_parser.add_argument("--project", help="GCP Project ID")
    status_parser.add_argument("--database", default="(default)", help="Firestore Database ID")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List all migrations for an environment")
    list_parser.add_argument("--env", required=True, choices=["dev", "prod"], help="Environment")
    list_parser.add_argument("--project", help="GCP Project ID")
    list_parser.add_argument("--database", default="(default)", help="Firestore Database ID")
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Show migration info from YAML")
    info_parser.add_argument("migration_id", help="Migration ID")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == "info":
        migration_info = get_migration_info(args.migration_id)
        if migration_info:
            print(f"\nMigration: {migration_info.get('name')}")
            print(f"ID: {migration_info.get('id')}")
            print(f"Script: {migration_info.get('script')}")
            print(f"Description: {migration_info.get('description')}")
            print(f"Status: {migration_info.get('status')}")
            print(f"Created: {migration_info.get('created_date')}")
            deps = migration_info.get('dependencies', [])
            if deps:
                print(f"Dependencies: {', '.join(deps)}")
            print(f"Reversible: {migration_info.get('reversible', False)}")
        else:
            print(f"Migration {args.migration_id} not found in {MIGRATIONS_YAML}")
        return
    
    # Determine project
    if args.command in ["apply", "status", "list"]:
        if args.project:
            project = args.project
        else:
            project = get_project_for_env(args.env)
        
        db = firestore.Client(project=project, database=args.database)
    
    if args.command == "apply":
        # Get migration info from YAML
        migration_info = get_migration_info(args.migration_id)
        if not migration_info:
            print(f"❌ Migration {args.migration_id} not found in {MIGRATIONS_YAML}")
            print("Please add it to migrations.yaml first")
            sys.exit(1)
        
        # Check dependencies
        if not args.skip_deps:
            if not check_dependencies(args.migration_id, args.env, project):
                print(f"\n❌ Dependencies not satisfied. Use --skip-deps to override.")
                sys.exit(1)
        
        record_migration(
            db=db,
            migration_id=args.migration_id,
            migration_name=migration_info.get("name", args.migration_id),
            script_path=migration_info.get("script", ""),
            description=migration_info.get("description", ""),
            applied_by=args.by,
            environment=args.env,
            status="applied"
        )
        
    elif args.command == "status":
        status = get_migration_status(db, args.migration_id, args.env)
        if status:
            print(f"\nMigration: {status.get('migration_name')}")
            print(f"ID: {status.get('migration_id')}")
            print(f"Environment: {status.get('environment')}")
            print(f"Status: {status.get('status')}")
            print(f"Applied: {status.get('applied_at')}")
            print(f"By: {status.get('applied_by')}")
        else:
            print(f"Migration {args.migration_id} not found in {args.env} environment")
    
    elif args.command == "list":
        migrations = list_migrations(db, args.env)
        if not migrations:
            print(f"No migrations recorded for {args.env} environment")
        else:
            print(f"\nFound {len(migrations)} migrations in {args.env}:\n")
            for m in migrations:
                print(f"  {m.get('migration_id')}: {m.get('migration_name')}")
                print(f"    Status: {m.get('status')}")
                print(f"    Applied: {m.get('applied_at')}")
                print(f"    By: {m.get('applied_by')}")
                print()


if __name__ == "__main__":
    main()

