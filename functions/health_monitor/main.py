# main.py
# Health Monitor for Redirector Functions
# - Checks health endpoints for Cloudflare Worker and GCP Function (dev & prod)
# - Performs test redirect scans via both paths (Worker and Direct)
# - Verifies data is correctly written to Firestore
# - Logs errors for Cloud Monitoring alerts

import os
import time
import hmac
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from flask import Request, jsonify
import functions_framework
import requests
from google.cloud import firestore

# Optional: verify Firebase ID token
try:
    import firebase_admin
    from firebase_admin import auth as fb_auth
    firebase_admin.initialize_app()  # uses default credentials
except Exception:
    fb_auth = None  # if you prefer IAM-only auth, handle below

# Initialize clients
_db = firestore.Client()

# Configuration from environment
PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT")
TEST_LINK_ID = os.environ.get("TEST_LINK_ID", "monitor-test-001")
WORKER_HMAC_SECRET = os.environ.get("WORKER_HMAC_SECRET", "")
CLOUDFLARE_WORKER_DEV_URL = os.environ.get("CLOUDFLARE_WORKER_DEV_URL", "https://dev.rocket-letter.de")
CLOUDFLARE_WORKER_PROD_URL = os.environ.get("CLOUDFLARE_WORKER_PROD_URL", "https://go.rocket-letter.de")
GCP_FUNCTION_DEV_URL = os.environ.get("GCP_FUNCTION_DEV_URL", "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/redirector")
GCP_FUNCTION_PROD_URL = os.environ.get("GCP_FUNCTION_PROD_URL", "https://europe-west3-gb-qr-tracker.cloudfunctions.net/redirector")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

# Detect environment (dev or prod) based on PROJECT_ID
IS_PROD = PROJECT_ID and "gb-qr-tracker" in PROJECT_ID and "dev" not in PROJECT_ID
IS_DEV = PROJECT_ID and "dev" in PROJECT_ID

# Additional domains to test (comma-separated list)
# Only test additional domains in prod (ihr-brief.de is only configured in prod)
ADDITIONAL_DOMAINS = os.environ.get("ADDITIONAL_DOMAINS", "")
if not ADDITIONAL_DOMAINS and IS_PROD:
    # Default to ihr-brief.de domains only in prod
    ADDITIONAL_DOMAINS = "ihr-brief.de,www.ihr-brief.de"
elif not ADDITIONAL_DOMAINS:
    # Empty for dev
    ADDITIONAL_DOMAINS = ""

# Strip quotes if present (gcloud may preserve them)
if ADDITIONAL_DOMAINS:
    if ADDITIONAL_DOMAINS.startswith('"') and ADDITIONAL_DOMAINS.endswith('"'):
        ADDITIONAL_DOMAINS = ADDITIONAL_DOMAINS[1:-1]
    elif ADDITIONAL_DOMAINS.startswith("'") and ADDITIONAL_DOMAINS.endswith("'"):
        ADDITIONAL_DOMAINS = ADDITIONAL_DOMAINS[1:-1]
    # Parse additional domains into a list
    ADDITIONAL_DOMAIN_LIST = [d.strip() for d in ADDITIONAL_DOMAINS.split(",") if d.strip()]
else:
    ADDITIONAL_DOMAIN_LIST = []

# Timeout for HTTP requests
REQUEST_TIMEOUT = 10  # seconds
DB_VERIFICATION_WINDOW = 300  # 5 minutes in seconds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Max-Age": "3600",
    }


def _generate_hmac_signature(link_id: str) -> Tuple[str, str]:
    """Generate HMAC signature for Cloudflare Worker request."""
    ts = str(int(time.time()))
    msg = f"{ts}:{link_id}"
    
    # Clean secret (remove quotes if present)
    secret = WORKER_HMAC_SECRET
    if secret and ((secret.startswith("'") and secret.endswith("'")) or 
                   (secret.startswith('"') and secret.endswith('"'))):
        secret = secret[1:-1]
    
    sig = hmac.new(
        secret.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    return ts, sig


def _check_health_endpoint(url: str, name: str) -> Tuple[bool, Optional[str]]:
    """Check a health endpoint. Returns (success, error_message)."""
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        if response.status_code == 200 and response.text.strip().lower() == "ok":
            return True, None
        else:
            return False, f"Expected 200 with 'ok' body, got {response.status_code}: {response.text[:100]}"
    except requests.exceptions.Timeout:
        return False, "Request timeout"
    except requests.exceptions.RequestException as e:
        return False, f"Request failed: {str(e)}"


def _perform_test_scan_worker(worker_url: str, link_id: str, env: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Perform test scan via Cloudflare Worker.
    Returns (success, error_message, response_info).
    """
    try:
        # Generate HMAC signature
        ts, sig = _generate_hmac_signature(link_id)
        
        # Build request URL
        test_url = f"{worker_url.rstrip('/')}/{link_id}"
        
        # Make request with HMAC headers
        headers = {
            "x-ts": ts,
            "x-sig": sig,
            "User-Agent": "HealthMonitor/1.0",
        }
        
        response = requests.get(
            test_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False
        )
        
        # Check for redirect (302 or 301)
        if response.status_code in (301, 302, 307, 308):
            location = response.headers.get("Location", "")
            return True, None, {
                "status_code": response.status_code,
                "location": location,
                "path": "worker",
                "environment": env
            }
        else:
            return False, f"Expected redirect (301/302), got {response.status_code}: {response.text[:100]}", None
            
    except requests.exceptions.Timeout:
        return False, "Request timeout", None
    except Exception as e:
        return False, f"Request failed: {str(e)}", None


def _perform_test_scan_direct(gcp_url: str, link_id: str, env: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Perform test scan via direct GCP Function.
    Returns (success, error_message, response_info).
    """
    try:
        # Build request URL with query parameter
        test_url = f"{gcp_url.rstrip('/')}?id={link_id}"
        
        # Make direct request (no HMAC signature)
        headers = {
            "User-Agent": "HealthMonitor/1.0",
        }
        
        response = requests.get(
            test_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False
        )
        
        # Check for redirect (302 or 301)
        if response.status_code in (301, 302, 307, 308):
            location = response.headers.get("Location", "")
            return True, None, {
                "status_code": response.status_code,
                "location": location,
                "path": "direct",
                "environment": env
            }
        else:
            return False, f"Expected redirect (301/302), got {response.status_code}: {response.text[:100]}", None
            
    except requests.exceptions.Timeout:
        return False, "Request timeout", None
    except Exception as e:
        return False, f"Request failed: {str(e)}", None


def _verify_hit_in_database(link_id: str, expected_origin: str, scan_time: datetime) -> Tuple[bool, Optional[str], Optional[Dict], Optional[firestore.DocumentReference]]:
    """
    Verify that a test hit was written to Firestore test_hits collection.
    Returns (success, error_message, hit_data, hit_doc_ref).
    
    Note: For test requests, hits are written to test_hits collection and counters
    are not updated, so we only verify the hit exists, not the link counters.
    """
    try:
        # Calculate time window for search
        window_start = scan_time - timedelta(seconds=DB_VERIFICATION_WINDOW)
        
        # Query test_hits collection for recent hits with this link_id
        # Test hits are written to test_hits collection to avoid polluting production data
        # Note: We query without order_by to avoid index requirements, then sort in Python
        hits_ref = _db.collection('test_hits')
        query = hits_ref.where('link_id', '==', link_id).limit(20)
        
        hits = list(query.stream())
        
        # Find a hit within the time window with matching origin
        found_hit = None
        found_hit_ref = None
        for hit in hits:
            hit_data = hit.to_dict()
            hit_ts = hit_data.get('ts')
            
            # Check if timestamp is recent
            if hit_ts:
                # Handle Firestore Timestamp object - it has timestamp() method
                try:
                    if hasattr(hit_ts, 'timestamp'):
                        # Firestore Timestamp object
                        hit_timestamp = hit_ts.timestamp()
                    elif isinstance(hit_ts, datetime):
                        # Python datetime object
                        hit_timestamp = hit_ts.timestamp()
                    else:
                        hit_timestamp = None
                    
                    if hit_timestamp and hit_timestamp >= window_start.timestamp():
                        # Check origin matches
                        if hit_data.get('hit_origin') == expected_origin:
                            found_hit = hit_data
                            found_hit['hit_id'] = hit.id
                            found_hit_ref = hit.reference
                            break
                except Exception:
                    # Skip this hit if timestamp conversion fails
                    continue
        
        if not found_hit:
            return False, f"No matching test hit found in test_hits collection (link_id={link_id}, origin={expected_origin}, window={DB_VERIFICATION_WINDOW}s)", None, None
        
        # Verify link document exists (but don't check counters since we skip updating them for test requests)
        link_ref = _db.collection('links').document(link_id)
        link_doc = link_ref.get()
        
        if not link_doc.exists:
            return False, f"Link document {link_id} not found", found_hit, found_hit_ref
        
        # For test requests, we skip counter updates, so we only verify the hit was written
        # The hit existing in test_hits collection is sufficient verification
        return True, None, {
            "hit": found_hit,
            "link": {
                "exists": True,
                "note": "Counters not updated for test requests"
            }
        }, found_hit_ref
        
    except Exception as e:
        return False, f"Database verification failed: {str(e)}", None, None


def _delete_hit(hit_ref: Optional[firestore.DocumentReference]) -> bool:
    """
    Delete a hit document from Firestore.
    Returns True if successful, False otherwise.
    
    Note: Test hits are now written to test_hits collection and can remain there
    for debugging purposes. This function is kept for backwards compatibility
    but deletion is no longer performed automatically.
    """
    # Test hits are now in test_hits collection and can remain for debugging
    # No need to delete them automatically
    return True


def _verify_firebase_token(request: Request) -> str:
    """Verify Firebase ID token and return user ID."""
    if fb_auth is None:
        raise PermissionError("Auth not configured on server (firebase_admin missing).")
    authz = request.headers.get("Authorization", "")
    if not authz.startswith("Bearer "):
        raise PermissionError("Missing bearer token")
    id_token = authz.split(" ", 1)[1].strip()
    decoded = fb_auth.verify_id_token(id_token, check_revoked=True)
    return decoded["uid"]


def _authenticate_request(request: Request) -> str:
    """
    Authenticate request from either frontend (Firebase ID token) or Cloud Scheduler.
    
    Cloud Scheduler requests are identified by User-Agent header and skip Firebase token
    verification since Cloud Functions already validates IAM permissions for service accounts.
    
    Returns user/service identifier.
    """
    user_agent = request.headers.get("User-Agent", "")
    
    # Cloud Scheduler requests - skip token verification (IAM is already validated by Cloud Functions)
    if "Google-Cloud-Scheduler" in user_agent:
        return "cloud-scheduler"
    
    # Frontend requests - verify Firebase ID token
    return _verify_firebase_token(request)


def _log_error(component: str, check_type: str, error: str, details: Optional[Dict] = None):
    """Log error for Cloud Monitoring alerts."""
    error_msg = f"[HEALTH_MONITOR_FAIL] {component} - {check_type}: {error}"
    if details:
        error_msg += f" | Details: {details}"
    
    logger.error(error_msg, extra={
        "component": component,
        "check_type": check_type,
        "error": error,
        "details": details or {}
    })


def _check_all_health_endpoints() -> Dict:
    """Check health endpoints for the current environment only."""
    # Determine current environment
    current_env = "dev" if IS_DEV else ("prod" if IS_PROD else "unknown")
    
    if current_env == "unknown":
        return {
            "error": "Could not determine environment from PROJECT_ID",
            "worker": {"success": False, "error": "Unknown environment"},
            "gcp": {"success": False, "error": "Unknown environment"}
        }
    
    results = {}
    
    # Select URLs based on current environment
    if current_env == "dev":
        worker_url = CLOUDFLARE_WORKER_DEV_URL
        gcp_url = GCP_FUNCTION_DEV_URL
    else:  # prod
        worker_url = CLOUDFLARE_WORKER_PROD_URL
        gcp_url = GCP_FUNCTION_PROD_URL
    
    # Check Cloudflare Worker
    success, error = _check_health_endpoint(
        f"{worker_url}/health",
        f"Cloudflare Worker ({current_env})"
    )
    results["worker"] = {"success": success, "error": error}
    if not success:
        _log_error("cloudflare_worker", "health_check", error or "Unknown error", {"environment": current_env})
    
    # Check additional domains (only in prod)
    if current_env == "prod":
        for domain in ADDITIONAL_DOMAIN_LIST:
            domain_key = domain.replace(".", "_").replace("-", "_")
            domain_url = f"https://{domain}"
            success, error = _check_health_endpoint(
                f"{domain_url}/health",
                f"Cloudflare Worker ({domain})"
            )
            results[f"worker_{domain_key}"] = {"success": success, "error": error}
            if not success:
                _log_error("cloudflare_worker", "health_check", error or "Unknown error", {"domain": domain})
    
    # Check GCP Function
    success, error = _check_health_endpoint(
        f"{gcp_url}/health",
        f"GCP Function ({current_env})"
    )
    results["gcp"] = {"success": success, "error": error}
    if not success:
        _log_error("gcp_function", "health_check", error or "Unknown error", {"environment": current_env})
    
    return results


def _perform_test_scans() -> Dict:
    """Perform test scans for the current environment only and verify database."""
    # Determine current environment
    current_env = "dev" if IS_DEV else ("prod" if IS_PROD else "unknown")
    
    if current_env == "unknown":
        return {
            "error": "Could not determine environment from PROJECT_ID",
            "worker": {"success": False, "error": "Unknown environment", "db_verified": False},
            "direct": {"success": False, "error": "Unknown environment", "db_verified": False}
        }
    
    results = {}
    
    # Select URLs based on current environment
    if current_env == "dev":
        worker_url = CLOUDFLARE_WORKER_DEV_URL
        gcp_url = GCP_FUNCTION_DEV_URL
    else:  # prod
        worker_url = CLOUDFLARE_WORKER_PROD_URL
        gcp_url = GCP_FUNCTION_PROD_URL
    
    scan_time = datetime.now(timezone.utc)
    
    # Test via Cloudflare Worker
    success, error, response_info = _perform_test_scan_worker(
        worker_url,
        TEST_LINK_ID,
        current_env
    )
    results["worker"] = {
        "success": success,
        "error": error,
        "response": response_info,
        "db_verified": False
    }
    
    if success:
        # Verify in database
        db_success, db_error, db_data, hit_ref = _verify_hit_in_database(
            TEST_LINK_ID,
            "cloudflare_worker",
            scan_time
        )
        results["worker"]["db_verified"] = db_success
        if not db_success:
            results["worker"]["db_error"] = db_error
            _log_error("cloudflare_worker", "db_verification", db_error or "Unknown error", {
                "environment": current_env,
                "link_id": TEST_LINK_ID
            })
    else:
        _log_error("cloudflare_worker", "test_scan", error or "Unknown error", {
            "environment": current_env,
            "link_id": TEST_LINK_ID
        })
    
    # Test additional domains via Cloudflare Worker (only in prod)
    if current_env == "prod":
        for domain in ADDITIONAL_DOMAIN_LIST:
            domain_key = domain.replace(".", "_").replace("-", "_")
            domain_url = f"https://{domain}"
            
            success, error, response_info = _perform_test_scan_worker(
                domain_url,
                TEST_LINK_ID,
                domain
            )
            results[f"worker_{domain_key}"] = {
                "success": success,
                "error": error,
                "response": response_info,
                "db_verified": False
            }
            
            if success:
                # Verify in database
                db_success, db_error, db_data, hit_ref = _verify_hit_in_database(
                    TEST_LINK_ID,
                    "cloudflare_worker",
                    scan_time
                )
                results[f"worker_{domain_key}"]["db_verified"] = db_success
                if not db_success:
                    results[f"worker_{domain_key}"]["db_error"] = db_error
                    _log_error("cloudflare_worker", "db_verification", db_error or "Unknown error", {
                        "domain": domain,
                        "link_id": TEST_LINK_ID
                    })
            else:
                _log_error("cloudflare_worker", "test_scan", error or "Unknown error", {
                    "domain": domain,
                    "link_id": TEST_LINK_ID
                })
    
    # Test via Direct GCP Function
    success, error, response_info = _perform_test_scan_direct(
        gcp_url,
        TEST_LINK_ID,
        current_env
    )
    results["direct"] = {
        "success": success,
        "error": error,
        "response": response_info,
        "db_verified": False
    }
    
    if success:
        # Verify in database
        db_success, db_error, db_data, hit_ref = _verify_hit_in_database(
            TEST_LINK_ID,
            "direct",
            scan_time
        )
        results["direct"]["db_verified"] = db_success
        if not db_success:
            results["direct"]["db_error"] = db_error
            _log_error("gcp_function", "db_verification", db_error or "Unknown error", {
                "environment": current_env,
                "link_id": TEST_LINK_ID,
                "path": "direct"
            })
    else:
        _log_error("gcp_function", "test_scan", error or "Unknown error", {
            "environment": current_env,
            "link_id": TEST_LINK_ID,
            "path": "direct"
        })
    
    return results


@functions_framework.http
def health_monitor(request: Request):
    """
    Main entry point for health monitoring.
    Checks health endpoints, performs test scans, and verifies database.
    
    Auth:
      - Frontend: Expects Firebase ID token in Authorization: Bearer <token>
      - Cloud Scheduler: Identified by User-Agent header, IAM permissions validated by Cloud Functions
    """
    # Handle CORS preflight
    if request.method == "OPTIONS":
        return ("", 204, _cors_headers())

    try:
        # Authenticate request (handles both Firebase ID tokens and Cloud Scheduler)
        try:
            user_id = _authenticate_request(request)
            logger.info(f"Health monitor check authenticated for: {user_id}")
        except Exception as e:
            logger.warning(f"Authentication failed: {str(e)}")
            return (jsonify({"error": f"Unauthorized: {str(e)}"}), 401, _cors_headers())
        
        logger.info("Starting health monitor check")
        
        # Perform health checks
        health_results = _check_all_health_endpoints()
        
        # Perform test scans with database verification
        test_results = _perform_test_scans()
        
        # Compile summary
        # Filter out any error entries and get only actual check results
        health_check_results = {k: v for k, v in health_results.items() if k != "error" and isinstance(v, dict) and "success" in v}
        test_scan_results = {k: v for k, v in test_results.items() if k != "error" and isinstance(v, dict) and "success" in v}
        
        all_health_ok = all(r.get("success", False) for r in health_check_results.values()) if health_check_results else False
        all_tests_ok = all(r.get("success", False) for r in test_scan_results.values()) if test_scan_results else False
        all_db_verified = all(r.get("db_verified", False) for r in test_scan_results.values()) if test_scan_results else False
        
        overall_success = all_health_ok and all_tests_ok and all_db_verified
        
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": "dev" if IS_DEV else ("prod" if IS_PROD else "unknown"),
            "overall_success": overall_success,
            "health_checks": health_results,
            "test_scans": test_results,
            "summary": {
                "all_health_ok": all_health_ok,
                "all_tests_ok": all_tests_ok,
                "all_db_verified": all_db_verified,
            }
        }
        
        if overall_success:
            logger.info("Health monitor check passed")
            logger.info(f"Result: {result}")
            return (jsonify(result), 200, _cors_headers())
        else:
            logger.warning("Health monitor check failed", extra={"result": result})
            return (jsonify(result), 500, _cors_headers())
            
    except Exception as e:
        error_msg = f"Health monitor check failed with exception: {str(e)}"
        logger.exception(error_msg)
        _log_error("health_monitor", "exception", error_msg)
        return (jsonify({"error": error_msg}), 500, _cors_headers())

