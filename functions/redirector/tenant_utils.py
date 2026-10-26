"""Pure helpers for Worker hostname handling (easy to unit test without Firestore)."""

import re

_TENANT_POOL_RE = re.compile(r"^[a-z0-9-]{1,40}$")
_CHECK_MODES = ("off", "log_only", "enforce")


def normalize_original_host(header_val: str) -> str:
    if not header_val or not str(header_val).strip():
        return ""
    return str(header_val).strip().lower().split(":", 1)[0]


def tenant_pool(tenant_id: str | None) -> str | None:
    """Pool name used in links/{pool}__{code}. None when the value cannot be a pool."""
    if not isinstance(tenant_id, str):
        return None
    pool = tenant_id.strip().lower()
    if not pool or not _TENANT_POOL_RE.fullmatch(pool):
        return None
    return pool


def domain_link_pool(tenant_id: str | None, shared: bool) -> str | None:
    """
    Pool whose links a customer domain opens first. Shared domains have none:
    several customers print on them, so they only open bare links/{code}.
    Must match upload_processor.printed_host_pool.
    """
    if shared:
        return None
    return tenant_pool(tenant_id)


def domain_tenant_check_mode(raw: str | None) -> str:
    """off | log_only | enforce. Missing or unknown values stay off."""
    mode = (raw or "off").strip().lower()
    if mode not in _CHECK_MODES:
        return "off"
    return mode


def link_doc_ids_to_try(
    source: str,
    short_code: str,
    host_tenant: str | None,
    request_tenant: str | None,
) -> list[str]:
    """
    Firestore document ids to open, in order.

    Worker traffic uses the domain's tenant and then the legacy bare id.
    A direct request with ?tenant= opens only links/{tenant}__{code}.
    A direct request without that parameter opens the legacy bare id.
    An invalid ?tenant= value yields no ids, so the caller returns 404
    instead of another tenant's document.
    """
    if source == "cloudflare_worker":
        ids: list[str] = []
        pool = tenant_pool(host_tenant)
        if pool:
            ids.append(f"{pool}__{short_code}")
        ids.append(short_code)
        return ids

    raw = request_tenant.strip() if isinstance(request_tenant, str) else ""
    if raw:
        pool = tenant_pool(raw)
        if not pool:
            return []
        return [f"{pool}__{short_code}"]
    return [short_code]
