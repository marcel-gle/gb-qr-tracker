import importlib.util
from pathlib import Path

UTILS = Path(__file__).resolve().parents[1] / "functions" / "redirector" / "tenant_utils.py"


def _load():
    spec = importlib.util.spec_from_file_location("tenant_utils_under_test", UTILS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_tries_the_host_pool_then_the_bare_id():
    tu = _load()
    assert tu.link_doc_ids_to_try("cloudflare_worker", "abc", "Rocket-Letter", "ignored") == [
        "rocket-letter__abc",
        "abc",
    ]


def test_worker_without_a_domain_doc_uses_the_bare_id():
    tu = _load()
    assert tu.link_doc_ids_to_try("cloudflare_worker", "abc", None, "rocket-letter") == ["abc"]


def test_direct_tenant_opens_only_that_pool():
    tu = _load()
    assert tu.link_doc_ids_to_try("direct", "abc", "other", "ihr-brief") == ["ihr-brief__abc"]


def test_direct_without_tenant_opens_the_bare_id():
    tu = _load()
    assert tu.link_doc_ids_to_try("direct", "abc", "rocket-letter", None) == ["abc"]
    assert tu.link_doc_ids_to_try("direct", "abc", None, "  ") == ["abc"]


def test_invalid_direct_tenant_does_not_fall_back():
    tu = _load()
    assert tu.link_doc_ids_to_try("direct", "abc", None, "not a tenant") == []


def test_check_mode_defaults_to_off():
    tu = _load()
    assert tu.domain_tenant_check_mode(None) == "off"
    assert tu.domain_tenant_check_mode(" enforce ") == "enforce"
    assert tu.domain_tenant_check_mode("sometimes") == "off"


def test_shared_domain_has_no_pool():
    tu = _load()
    assert tu.domain_link_pool("ihr-brief", True) is None
    assert tu.domain_link_pool("Rocket-Letter", False) == "rocket-letter"
    assert tu.domain_link_pool(None, False) is None
