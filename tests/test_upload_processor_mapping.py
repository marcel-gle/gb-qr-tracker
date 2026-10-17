import importlib.util
import sys
import types
from pathlib import Path

import pytest

MAIN = Path(__file__).resolve().parents[1] / "functions" / "upload_processor" / "main.py"


class _Anything:
    def __init__(self, *a, **k):
        pass

    def __call__(self, *a, **k):
        return _Anything()

    def __getattr__(self, name):
        return _Anything()


class _StubModule(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return _Anything


@pytest.fixture(scope="module")
def up(monkeypatch_module):
    google = _StubModule("google")
    cloud = _StubModule("google.cloud")
    firestore = _StubModule("google.cloud.firestore")
    firestore.SERVER_TIMESTAMP = "SERVER_TIMESTAMP"
    storage = _StubModule("google.cloud.storage")
    fs_v1 = _StubModule("google.cloud.firestore_v1")
    exceptions = _StubModule("google.api_core.exceptions")
    exceptions.AlreadyExists = type("AlreadyExists", (Exception,), {})
    api_core = _StubModule("google.api_core")
    ff = _StubModule("functions_framework")
    ff.cloud_event = lambda f: f
    cloud.firestore = firestore
    cloud.storage = storage
    stubs = {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.firestore": firestore,
        "google.cloud.storage": storage,
        "google.cloud.firestore_v1": fs_v1,
        "google.api_core": api_core,
        "google.api_core.exceptions": exceptions,
        "functions_framework": ff,
    }
    for name, mod in stubs.items():
        monkeypatch_module.setitem(sys.modules, name, mod)
    if "tldextract" not in sys.modules:
        monkeypatch_module.setitem(sys.modules, "tldextract", types.ModuleType("tldextract"))

    spec = importlib.util.spec_from_file_location("upload_processor_main", MAIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def _final_row(**overrides):
    row = {
        "company_name": "Muster GmbH",
        "street": "Hauptstr.",
        "house_number": "1",
        "postcode": "80331",
        "city": "München",
        "first_name_1": "Max",
        "last_name_1": "Mustermann",
        "salutation_1": "Herr",
        "first_name_2": "Erika",
        "last_name_2": "Muster",
        "salutation_2": "Frau",
        "email": "info@muster.de",
        "phone": "+49 89 123",
        "apollo_phone_1": "",
        "apollo_email_1": "",
        "apollo_email_status_1": "",
        "apollo_phone_2": "",
        "apollo_email_2": "",
        "apollo_email_status_2": "",
        "Template": "t1",
    }
    row.update(overrides)
    return row


def test_final_csv_names_and_salutation_are_uploaded(up):
    _, canonical, customer = up.upsert_business_payload_from_row(_final_row(), "owner", None)
    assert canonical["business_name"] == "Muster GmbH"
    assert customer["name"] == "Max Mustermann"
    assert customer["salutation"] == "Herr"
    assert customer["phone"] == "+49 89 123"
    assert customer["email"] == "info@muster.de"
    assert not any(k.startswith("contact_") for k in customer)


def test_legacy_columns_still_win(up):
    row = {"Namenszeile": "Alt GmbH", "PLZ": "1", "Entscheider 1 Vorname": "Hans",
           "Entscheider 1 Nachname": "Alt", "Entscheider 1 Anrede": "Herr"}
    _, _, customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert customer["name"] == "Hans Alt"
    assert customer["salutation"] == "Herr"


def test_empty_legacy_column_falls_through_to_final_column(up):
    row = _final_row(**{"Vorname": "", "Salutation": ""})
    _, _, customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert customer["name"] == "Max Mustermann"
    assert customer["salutation"] == "Herr"


def test_apollo_contact_is_extra_and_keeps_company_contact(up):
    row = _final_row(apollo_phone_1="+49 170 111", apollo_email_1="max@muster.de",
                     apollo_email_status_1="verified", apollo_title_1="Geschäftsführer")
    _, _, customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert customer["phone"] == "+49 89 123"
    assert customer["email"] == "info@muster.de"
    assert customer["contact_phone"] == "+49 170 111"
    assert customer["contact_email"] == "max@muster.de"
    assert customer["contact_name"] == "Max Mustermann"
    assert customer["contact_salutation"] == "Herr"
    assert customer["contact_title"] == "Geschäftsführer"
    assert customer["contact_source"] == "apollo"


def test_unverified_apollo_email_is_dropped(up):
    row = _final_row(apollo_email_1="guess@muster.de", apollo_email_status_1="extrapolated")
    _, _, customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert "contact_email" not in customer
    assert "contact_source" not in customer


def test_apollo_contact_uses_director_with_data(up):
    row = _final_row(apollo_phone_2="+49 171 222")
    _, _, customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert customer["name"] == "Max Mustermann"
    assert customer["contact_phone"] == "+49 171 222"
    assert customer["contact_name"] == "Erika Muster"
    assert customer["contact_salutation"] == "Frau"
