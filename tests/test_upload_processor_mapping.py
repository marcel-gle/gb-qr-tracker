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


def test_absatz_alias_becomes_paragraph_on_target_and_output(up, tmp_path):
    text = "Ein längerer Briefabsatz, mit Komma; und Semikolon."
    row = {"company_name": "A GmbH", "absatz": text, "Paragraph": ""}
    assert up.normalize_paragraph_column(row) == text
    assert row["Paragraph"] == text
    assert "absatz" not in row
    assert up.paragraph_field_for_target(row) == text

    names = up.csv_fieldnames_union([
        {**row, "tracking_link": "https://x/a", "tracking_url": "https://x/a", "tracking_id": "a"}
    ])
    assert "Paragraph" in names
    assert "absatz" not in names
    assert names[-3:] == ["tracking_link", "tracking_url", "tracking_id"]

    src = tmp_path / "list.csv"
    src.write_text("company_name,Paragraph\n", encoding="utf-8")
    out = up.write_back_csv(str(src), [row])
    written = Path(out).read_text(encoding="utf-8")
    assert "Paragraph" in written.splitlines()[0]
    assert text in written


def test_paragraph_column_is_optional(up):
    row = {"company_name": "A GmbH"}
    assert up.normalize_paragraph_column(row) is None
    assert "Paragraph" not in row
    assert up.paragraph_field_for_target(row) is None
    _, canonical, _customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert canonical["business_name"] == "A GmbH"


def test_apollo_contact_uses_director_with_data(up):
    row = _final_row(apollo_phone_2="+49 171 222")
    _, _, customer = up.upsert_business_payload_from_row(row, "owner", None)
    assert customer["name"] == "Max Mustermann"
    assert customer["contact_phone"] == "+49 171 222"
    assert customer["contact_name"] == "Erika Muster"
    assert customer["contact_salutation"] == "Frau"


class _LinkDoc:
    def __init__(self, store, doc_id, already_exists):
        self.store = store
        self.doc_id = doc_id
        self.already_exists = already_exists

    def create(self, payload):
        if self.doc_id in self.store:
            raise self.already_exists()
        self.store[self.doc_id] = payload


class _Links:
    def __init__(self, store, already_exists):
        self.store = store
        self.already_exists = already_exists

    def document(self, doc_id):
        return _LinkDoc(self.store, doc_id, self.already_exists)


@pytest.fixture
def link_store(up):
    previous = up.COL_LINKS
    store = {}
    up.COL_LINKS = _Links(store, up.AlreadyExists)
    yield store
    up.COL_LINKS = previous


def test_free_base_id_is_used_as_is(up, link_store):
    assert up.create_unique_link("beispiel", {"destination": "https://x"}, {}) == "beispiel"
    assert link_store["beispiel"]["short_code"] == "beispiel"


def test_collision_puts_the_number_in_front(up, link_store):
    link_store["beispiel"] = {"short_code": "beispiel"}
    link_store["1-beispiel"] = {"short_code": "1-beispiel"}
    next_n = {}

    assert up.create_unique_link("beispiel", {"destination": "https://x"}, next_n) == "2-beispiel"
    assert up.create_unique_link("beispiel", {"destination": "https://y"}, next_n) == "3-beispiel"
    assert link_store["2-beispiel"]["short_code"] == "2-beispiel"
    assert link_store["3-beispiel"]["destination"] == "https://y"
    assert next_n["beispiel"] == 4


def test_empty_base_falls_back_to_link(up, link_store):
    assert up.create_unique_link("  ", {}, {}) == "link"
    assert "link" in link_store


def test_hundredth_prefix_is_allowed_and_the_next_one_fails(up, link_store):
    link_store["cap"] = {}
    for n in range(1, 100):
        link_store[f"{n}-cap"] = {}

    assert up.create_unique_link("cap", {}, {}) == "100-cap"

    with pytest.raises(RuntimeError, match="after 100 numbered variants"):
        up.create_unique_link("cap", {}, {})
